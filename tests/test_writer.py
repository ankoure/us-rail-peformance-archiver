from datetime import datetime, timezone
import hashlib
import io
import json
import asyncio
import threading
from pathlib import Path
from dataclasses import dataclass, field
from archiver.writer import (
    BadHeaderError,
    CorruptFrameError,
    FrameReader,
    FrameWriter,
    LocalWriter,
    ContentAddressedWriter,
)
import pytest


@dataclass
class FakeWriteableResponse:
    """Minimal fake — only what LocalWriter actually reads."""

    _timestamp = datetime(2026, 5, 4, tzinfo=timezone.utc).timestamp()
    _payload: bytes
    _metadata: dict

    def raw_payload(self) -> bytes:
        return self._payload

    def to_metadata_row(self) -> dict:
        return self._metadata

    def get_datetime(self) -> datetime:
        return datetime.fromtimestamp(self._timestamp, tz=timezone.utc)

    def get_timestamp(self) -> float:
        return self._timestamp

    def content_digest(self) -> str:
        return hashlib.sha256(self._payload).hexdigest()


@dataclass
class FakeEmptyPayloadResponse(FakeWriteableResponse):
    def raw_payload(self) -> bytes:
        return None


async def test_writer_creates_raw_file_and_metadata(tmp_path):
    writer = LocalWriter(base_dir=str(tmp_path))
    response = FakeWriteableResponse(
        _payload=b"\x01\x02\x03",
        _metadata={"status_code": 200, "response_type": "ProtobufResponse"},
    )

    await writer.write(feed_name="test-feed", response=response)
    date = response.get_datetime()

    # raw file landed under the expected path
    raw_files = list(
        (
            tmp_path
            / "test-feed"
            / "raw"
            / f"year={date.year}"
            / f"month={date.month}"
            / f"day={date.day}"
        ).iterdir()
    )
    assert len(raw_files) == 1
    assert raw_files[0].suffix == ".bin"
    assert raw_files[0].read_bytes() == b"\x01\x02\x03"

    # metadata file has exactly one line, matching what we passed in
    metadata_lines = (
        (
            tmp_path
            / "test-feed"
            / "metadata"
            / f"year={date.year}"
            / f"month={date.month}"
            / f"day={date.day}"
            / "data.jsonl"
        )
        .read_text()
        .splitlines()
    )
    assert len(metadata_lines) == 1
    row = json.loads(metadata_lines[0])
    assert row == {"status_code": 200, "response_type": "ProtobufResponse"}


async def test_errorresponse_writes_no_bin(tmp_path):
    writer = LocalWriter(base_dir=str(tmp_path))
    response = FakeEmptyPayloadResponse(
        _payload=b"",
        _metadata={"status_code": 401, "response_type": "ErrorResponse"},
    )

    await writer.write(feed_name="test-feed", response=response)
    date = response.get_datetime()

    # raw file landed under the expected path
    raw_files = list(
        (
            tmp_path
            / "test-feed"
            / "raw"
            / f"year={date.year}"
            / f"month={date.month}"
            / f"day={date.day}"
        ).iterdir()
    )
    assert len(raw_files) == 0

    # metadata file has exactly one line, matching what we passed in
    metadata_lines = (
        (
            tmp_path
            / "test-feed"
            / "metadata"
            / f"year={date.year}"
            / f"month={date.month}"
            / f"day={date.day}"
            / "data.jsonl"
        )
        .read_text()
        .splitlines()
    )
    assert len(metadata_lines) == 1
    row = json.loads(metadata_lines[0])
    assert row == {"status_code": 401, "response_type": "ErrorResponse"}


def test_round_trip():
    # --- write ---
    payloads = [
        b"hello world",
        b"second frame",
        b"\x00\x01\x02\x03 binary data",
    ]

    buf = io.BytesIO()
    writer = FrameWriter(buf)
    for payload in payloads:
        digest = hashlib.sha256(payload).digest()
        writer.write_frame(payload, digest)

    # --- read ---
    buf.seek(0)
    frames = list(FrameReader(buf))

    assert len(frames) == 3
    for (payload, digest), expected in zip(frames, payloads):
        assert payload == expected
        assert digest == hashlib.sha256(expected).digest()

    print("round trip ok")


def test_corrupt_frame():
    buf = io.BytesIO()
    writer = FrameWriter(buf)
    payload = b"hello world"
    digest = hashlib.sha256(payload).digest()
    writer.write_frame(payload, digest)

    # flip a byte in the payload
    raw = bytearray(buf.getvalue())
    raw[-1] ^= 0xFF
    buf = io.BytesIO(bytes(raw))

    with pytest.raises(CorruptFrameError):
        list(FrameReader(buf))


def test_bad_magic():
    buf = io.BytesIO(b"\x00\x00\x00\x00\x00")
    with pytest.raises(BadHeaderError):
        list(FrameReader(buf))


# --------------------------------------------------------------------------
# ContentAddressedWriter
# --------------------------------------------------------------------------

FEED = "test-feed"


def _date_parts(response) -> str:
    date = response.get_datetime()
    return f"year={date.year}/month={date.month}/day={date.day}"


def _raw_dir(tmp_path: Path, response) -> Path:
    return tmp_path / FEED / "raw" / _date_parts(response)


def _bin_path(tmp_path: Path, response, payload: bytes) -> Path:
    return _raw_dir(tmp_path, response) / f"{hashlib.sha256(payload).hexdigest()}.bin"


def _metadata_lines(tmp_path: Path, response) -> list[str]:
    path = tmp_path / FEED / "metadata" / _date_parts(response) / "data.jsonl"
    return path.read_text().splitlines()


async def test_cas_writer_names_bin_by_content_digest(tmp_path):
    """The filename is the payload's sha256, not the response timestamp."""
    writer = ContentAddressedWriter(base_dir=str(tmp_path))
    payload = b"\x01\x02\x03"
    response = FakeWriteableResponse(
        _payload=payload,
        _metadata={"status_code": 200, "response_type": "ProtobufResponse"},
    )

    await writer.write(feed_name=FEED, response=response)

    raw_files = list(_raw_dir(tmp_path, response).iterdir())
    assert len(raw_files) == 1
    assert raw_files[0].name == f"{hashlib.sha256(payload).hexdigest()}.bin"
    assert raw_files[0].stem != str(response.get_timestamp())
    assert raw_files[0].read_bytes() == payload

    assert len(_metadata_lines(tmp_path, response)) == 1


async def test_identical_payloads_dedup_the_bin_but_not_the_metadata(tmp_path):
    """De-dup is bin-only.

    Two responses carrying the same bytes collapse onto one path — the
    filesystem is the de-dup mechanism — but append_metadata runs
    unconditionally, so both rows are still recorded.
    """
    writer = ContentAddressedWriter(base_dir=str(tmp_path))
    payload = b"identical bytes"
    first = FakeWriteableResponse(
        _payload=payload, _metadata={"status_code": 200, "seq": 1}
    )
    second = FakeWriteableResponse(
        _payload=payload, _metadata={"status_code": 200, "seq": 2}
    )

    await writer.write(feed_name=FEED, response=first)
    await writer.write(feed_name=FEED, response=second)

    # Comparing names (not just counting) also catches a stranded .bin.tmp.
    raw_files = sorted(p.name for p in _raw_dir(tmp_path, first).iterdir())
    assert raw_files == [f"{hashlib.sha256(payload).hexdigest()}.bin"]
    assert _bin_path(tmp_path, first, payload).read_bytes() == payload

    lines = _metadata_lines(tmp_path, first)
    assert len(lines) == 2
    assert [json.loads(line)["seq"] for line in lines] == [1, 2]


async def test_cas_writer_writes_no_bin_on_empty_payload(tmp_path):
    """A None payload writes metadata only.

    Note the difference from LocalWriter: that one mkdirs the raw directory
    before checking the payload, so its test can iterdir an empty dir. The CAS
    path only builds a path inside the `is not None` branch, so the raw
    directory is never created at all.
    """
    writer = ContentAddressedWriter(base_dir=str(tmp_path))
    response = FakeEmptyPayloadResponse(
        _payload=b"",
        _metadata={"status_code": 401, "response_type": "ErrorResponse"},
    )

    await writer.write(feed_name=FEED, response=response)

    assert not _raw_dir(tmp_path, response).exists()

    lines = _metadata_lines(tmp_path, response)
    assert len(lines) == 1
    assert json.loads(lines[0]) == {
        "status_code": 401,
        "response_type": "ErrorResponse",
    }


async def test_bin_is_on_disk_before_metadata_is_appended(tmp_path):
    """The ordering invariant, asserted as its consequence.

    Rather than only recording that one call preceded the other, this checks
    what the ordering is *for*: at the moment append_metadata is entered, the
    payload is already fully written and renamed into place, so a metadata row
    can never point at a missing or half-written .bin.
    """
    writer = ContentAddressedWriter(base_dir=str(tmp_path))
    payload = b"ordered"
    response = FakeWriteableResponse(_payload=payload, _metadata={"status_code": 200})
    bin_path = _bin_path(tmp_path, response, payload)

    calls: list[str] = []
    observed: dict = {}
    real_write_bytes_atomic = writer.write_bytes_atomic
    real_append_metadata = writer.append_metadata

    def spy_write_bytes_atomic(path, data):
        calls.append("bin")
        return real_write_bytes_atomic(path, data)

    def spy_append_metadata(feed_name, response):
        calls.append("metadata")
        observed["bin_contents"] = bin_path.read_bytes() if bin_path.exists() else None
        return real_append_metadata(feed_name, response)

    # Spies delegate to the real methods, so the writer still does its job and
    # the rest of the assertions are against genuine on-disk state.
    writer.write_bytes_atomic = spy_write_bytes_atomic
    writer.append_metadata = spy_append_metadata

    await writer.write(feed_name=FEED, response=response)

    assert calls == ["bin", "metadata"]
    assert observed["bin_contents"] == payload
    assert len(_metadata_lines(tmp_path, response)) == 1


@dataclass
class FakeBlockingResponse(FakeWriteableResponse):
    """Blocks inside raw_payload() until the test releases the gate.

    The timeout matters: if write() ever stops offloading, the fake unblocks
    itself and the test fails on _released instead of hanging the suite.
    """

    _gate: threading.Event = field(default_factory=threading.Event)
    _released: bool = False

    def raw_payload(self) -> bytes:
        self._released = self._gate.wait(timeout=5)
        return self._payload


async def test_write_keeps_the_event_loop_responsive(tmp_path):
    writer = ContentAddressedWriter(base_dir=str(tmp_path))
    payload = b"slow payload"
    response = FakeBlockingResponse(_payload=payload, _metadata={"status_code": 200})

    task = asyncio.create_task(writer.write(feed_name=FEED, response=response))

    # Only reachable while the write is in flight if the blocking work really
    # left the loop thread. Run it inline and the loop is pinned inside
    # raw_payload() until its timeout expires, so the gate is never set in time.
    await asyncio.sleep(0.05)
    response._gate.set()
    await task

    assert response._released is True
    assert _bin_path(tmp_path, response, payload).read_bytes() == payload
    assert len(_metadata_lines(tmp_path, response)) == 1
