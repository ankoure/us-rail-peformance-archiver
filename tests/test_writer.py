from datetime import datetime, timezone
import hashlib
import io
import json
from dataclasses import dataclass
from archiver.writer import (
    BadHeaderError,
    CorruptFrameError,
    FrameReader,
    FrameWriter,
    LocalWriter,
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
