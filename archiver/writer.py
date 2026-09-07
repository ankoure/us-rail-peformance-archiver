from abc import ABC
import hashlib
import io
import asyncio
from pathlib import Path
import shutil
import struct
from typing import BinaryIO, Iterable

from archiver.response import FeedResponse
from archiver.logger import logger
import json
from abc import abstractmethod


class BaseWriter(ABC):
    def __init__(self, base_dir: str) -> None:
        self.base_dir = Path(base_dir)

    def write_bytes_atomic(self, path: Path, data: bytes) -> None:
        tmp_path = path.with_name(path.name + ".tmp")
        tmp_path.parent.mkdir(parents=True, exist_ok=True)
        with open(tmp_path, "wb") as f:
            f.write(data)
        tmp_path.rename(path)

    def append_metadata(self, feed_name: str, response: FeedResponse) -> None:
        date = response.get_datetime()
        metadata_path = (
            self.base_dir
            / feed_name
            / "metadata"
            / f"year={date.year}"
            / f"month={date.month}"
            / f"day={date.day}"
            / "data.jsonl"
        )
        metadata_path.parent.mkdir(parents=True, exist_ok=True)
        record = response.to_metadata_row()
        with metadata_path.open("a") as f:
            f.write(json.dumps(record) + "\n")

    @abstractmethod
    async def write(self, feed_name: str, response: FeedResponse) -> None: ...


class LocalWriter(BaseWriter):
    def __init__(self, base_dir: str) -> None:
        super().__init__(base_dir)

    async def write(self, feed_name: str, response: FeedResponse) -> None:
        date = response.get_datetime()
        file_path = (
            self.base_dir
            / feed_name
            / "raw"
            / f"year={date.year}"
            / f"month={date.month}"
            / f"day={date.day}"
            / f"{response.get_timestamp()}.bin"
        )
        file_path.parent.mkdir(parents=True, exist_ok=True)

        payload = response.raw_payload()
        if payload is not None:
            self.write_bytes_atomic(file_path, payload)
        else:
            logger.info("No content to persist")

        self.append_metadata(feed_name, response)


MAGIC = b"\x89GRT"
VERSION = 0x01
HEADER = MAGIC + bytes([VERSION])


class FrameError(Exception):
    """Base class for all frame errors. Catching this means the object is untrustworthy."""


class BadHeaderError(FrameError):
    """Magic bytes or version number did not match. The whole file is suspect."""


class TruncatedFrameError(FrameError):
    """A short read occurred mid-frame. The write was likely interrupted."""


class CorruptFrameError(FrameError):
    """Digest mismatch. The payload does not match its stored digest."""

    def __init__(self, stored: bytes, computed: bytes):
        self.stored = stored
        self.computed = computed
        super().__init__(
            f"Digest mismatch: stored={stored.hex()}, computed={computed.hex()}"
        )


def _consume_header(stream: BinaryIO) -> None:
    """Read and validate the 5-byte header, leaving stream positioned at frame 0.

    Shared by FrameReader and the merge path so the magic/version check can't
    drift between them.
    """
    header = stream.read(5)
    if len(header) < 5:
        raise EOFError("Stream too short to contain a header")
    magic, version = header[:4], header[4]
    if magic != MAGIC:
        raise BadHeaderError(f"Bad magic: expected {MAGIC!r}, got {magic!r}")
    if version != VERSION:
        raise BadHeaderError(f"Unsupported version: 0x{version:02x}")


def _window_order(paths: Iterable[Path]) -> list[Path]:
    """Sort window objects by their window= timestamp, oldest first."""
    return sorted(paths, key=lambda p: int(p.stem.split("=")[1]))


def merge_frames_into(
    dest: BinaryIO, paths: Iterable[Path], *, chunk: int = 1 << 20
) -> int:
    """Stream frames from multiple window .bin files into dest.

    Writes one header then appends the raw frame bytes from each file in
    ascending window-timestamp order (oldest frames first), never holding more
    than `chunk` bytes of any source in memory.  Returns bytes written.  The
    caller is responsible for ensuring all paths share the same feed and hour
    bucket.

    Each source's own 5-byte header is validated and consumed rather than
    blindly skipped, so a source with bad magic or an unknown version raises
    BadHeaderError here instead of producing an object that only fails when
    something later tries to read it back.

    `dest` must be seekable (the return value is computed from tell()).
    """
    start = dest.tell()
    dest.write(HEADER)
    for path in _window_order(paths):
        with path.open("rb") as fh:
            _consume_header(fh)
            shutil.copyfileobj(fh, dest, chunk)
    return dest.tell() - start


def merge_lines_into(
    dest: BinaryIO, paths: Iterable[Path], *, chunk: int = 1 << 20
) -> int:
    """Stream multiple window .jsonl files into dest.

    Plain end-to-end concatenation in ascending window-timestamp order — unlike
    the .bin path there is no container header to write or strip, since each
    source is already newline-terminated records.  Returns bytes written.

    `dest` must be seekable (the return value is computed from tell()).
    """
    start = dest.tell()
    for path in _window_order(paths):
        with path.open("rb") as fh:
            shutil.copyfileobj(fh, dest, chunk)
    return dest.tell() - start


def merge_bins(paths: Iterable[Path]) -> bytes:
    """Concatenate frames from multiple window .bin files into one.

    DEPRECATED: materialises the whole merged object in memory, which is what
    the streaming path exists to avoid.  Kept only until the _merge_and_ship
    call site moves to merge_frames_into; delete both this and its two tests
    then.  Implemented as a wrapper so the two paths cannot diverge.
    """
    buf = io.BytesIO()
    merge_frames_into(buf, paths)
    return buf.getvalue()


class FrameWriter:
    def __init__(self, stream):
        self._stream = stream
        self._stream.write(HEADER)

    def write_frame(self, payload, digest):
        if len(digest) != 32:
            raise ValueError(f"digest must be 32 bytes, got {len(digest)}")
        self._stream.write(struct.pack(">I", len(payload)))
        self._stream.write(digest)
        self._stream.write(payload)


class FrameReader:
    def __init__(self, stream):
        self._stream = stream
        _consume_header(self._stream)

    def _read_exact(self, n):
        buf = self._stream.read(n)
        if len(buf) == 0:
            return None
        if len(buf) < n:
            raise TruncatedFrameError(
                f"Truncated stream: wanted {n} bytes, got {len(buf)}"
            )
        return buf

    def _read_frame(self):
        raw_len = self._read_exact(4)
        if raw_len is None:
            return None

        (payload_len,) = struct.unpack(">I", raw_len)

        digest = self._read_exact(32)
        if digest is None:
            raise EOFError("Truncated stream: missing digest")

        payload = self._read_exact(payload_len)
        if payload is None:
            raise EOFError("Truncated stream: missing payload")

        actual = hashlib.sha256(payload).digest()
        if actual != digest:
            raise CorruptFrameError(stored=digest, computed=actual)

        return payload, digest

    def __iter__(self):
        while True:
            result = self._read_frame()
            if result is None:
                return
            yield result


class ContentAddressedWriter(BaseWriter):
    """Writes each response to disk immediately, named by content digest.

    No in-memory buffer and no de-dup dict: identical content resolves to
    the same {digest}.bin path, so the filesystem is the de-dup mechanism.
    Batching for S3 is now purely LandingUploader's concern.
    """

    async def write(self, feed_name: str, response: FeedResponse) -> None:
        await asyncio.to_thread(self._write_sync, feed_name, response)

    def _write_sync(self, feed_name: str, response: FeedResponse) -> None:
        payload = response.raw_payload()
        if payload is not None:
            date = response.get_datetime()
            digest_hex = response.content_digest()  # sha256 hexdigest, str
            path = (
                self.base_dir
                / feed_name
                / "raw"
                / f"year={date.year}"
                / f"month={date.month}"
                / f"day={date.day}"
                / f"{digest_hex}.bin"
            )
            self.write_bytes_atomic(path, payload)
        else:
            logger.info("No content to persist")

        # AFTER the bin write. Same ordering invariant BatchingWriter held:
        # a metadata row must never point at a payload that isn't on disk yet.
        # Guaranteed here by sequential execution inside the one to_thread hop.
        self.append_metadata(feed_name, response)
