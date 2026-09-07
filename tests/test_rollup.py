import base64
from dataclasses import asdict, dataclass
from datetime import date, datetime
import hashlib
import io
import json
from pathlib import Path
from typing import Iterator
from archiver.feed import Feed
import pyarrow.parquet as pq
import pyarrow as pa
from archiver.decoder import VehicleRow, StandardDecoder
from archiver.payloads import digest_timestamps
from archiver.rollup import _schema_for_spec, Rollup
from archiver.writer import FrameWriter
import pytest
from archiver.decoder import Row, Decoder, TableSpec
from archiver.parser import Parser
from archiver.source import LocalSource
import logging

from archiver.payloads import iter_payloads


class FakeParser(Parser):
    def parse(self, body: bytes):
        return b""  # FakeDecoder ignores its input, so this can be anything


class EchoParser(Parser):
    def parse(self, body: bytes) -> bytes:
        return body  # identity: hand the raw frame payload straight to EchoDecoder


@dataclass
class EchoRow(Row):
    payload: str
    fetched_at: int | None


class EchoDecoder(Decoder):
    """One row per payload, echoing the bytes + the fetched_at it was given. Lets a
    test prove per-frame parsing AND that fetched_at is the joined/fallback value."""

    produces = {EchoRow: TableSpec("echoes")}

    def decode(self, raw: bytes, *, fetched_at: int | None = None) -> Iterator[Row]:
        yield EchoRow(payload=raw.decode(), fetched_at=fetched_at)


def _framed_bytes(payloads: list[bytes]) -> bytes:
    """Encode payloads as one BatchingWriter-style framed object (header + frames)."""
    buf = io.BytesIO()
    writer = FrameWriter(buf)
    for payload in payloads:
        writer.write_frame(payload, hashlib.sha256(payload).digest())
    return buf.getvalue()


def _write_framed_window(
    landing_dir, feed_name: str, day: date, window_start: int, payloads: list[bytes]
):
    path = (
        landing_dir
        / feed_name
        / "raw"
        / f"year={day.year}"
        / f"month={day.month}"
        / f"day={day.day}"
        / f"window={window_start}.bin"
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(_framed_bytes(payloads))
    return path


def _write_metadata(landing_dir, feed_name: str, day: date, rows: list[dict]):
    path = (
        landing_dir
        / feed_name
        / "metadata"
        / f"year={day.year}"
        / f"month={day.month}"
        / f"day={day.day}"
        / "data.jsonl"
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w") as f:
        for row in rows:
            f.write(json.dumps(row) + "\n")
    return path


def _echo_feed() -> Feed:
    return Feed(
        name="echo-feed",
        path="/whatever",
        parser=EchoParser(),
        decoder=EchoDecoder(),
        agency_id="A",
        poll_interval_seconds=60,
    )


@dataclass
class FakeRow(Row):
    feed_timestamp: int
    destination: str
    direction: str


class FakeDecoder(Decoder):
    produces = {FakeRow: TableSpec("fakes")}

    def decode(self, raw: bytes, *, fetched_at: int | None = None) -> Iterator[Row]:

        for _ in range(5):
            feed_timestamp = datetime.now().timestamp()
            destination = "Hollywood Hills"
            direction = "West"
            yield (
                FakeRow(
                    feed_timestamp=feed_timestamp,
                    destination=destination,
                    direction=direction,
                )
            )


# tests/test_rollup.py
def test_schema_for_spec_renames_and_extras():
    spec = StandardDecoder.produces[VehicleRow]
    schema = _schema_for_spec(VehicleRow, spec)
    field_types = {f.name: f.type for f in schema}
    # All proto-derived fields use LAMP's dotted names
    assert field_types["vehicle.vehicle.id"] == pa.string()
    assert field_types["vehicle.timestamp"] == pa.int64()
    assert field_types["vehicle.trip.direction_id"] == pa.int64()
    assert field_types["vehicle.position.latitude"] == pa.float64()
    assert field_types["vehicle.position.speed"] == pa.float64()
    assert field_types["vehicle.occupancy_status"] == pa.string()
    # null-pad extras present with expected types
    assert field_types["vehicle.trip.start_time"] == pa.string()
    assert field_types["vehicle.trip.revenue"] == pa.bool_()
    assert "vehicle.vehicle.consist" in field_types
    # feed_timestamp is added by the rollup, not from the proto — stays flat
    assert field_types["feed_timestamp"] == pa.int64()
    # original Python identifiers are gone
    assert "vehicle_id" not in field_types
    assert "vehicle_timestamp" not in field_types
    assert "latitude" not in field_types
    # all fields nullable
    assert all(f.nullable for f in schema)


def test_schema_for_spec_drift_check_fails_loudly():
    bad_spec = TableSpec("bad", column_names={"not_a_real_field": "x.y"})
    with pytest.raises(ValueError, match="not_a_real_field"):
        _schema_for_spec(VehicleRow, bad_spec)


def test_streaming_writer_empty_writes_nothing(tmp_path):
    out = tmp_path / "data.parquet"
    schema = _schema_for_spec(FakeRow, TableSpec("fakes"))
    with Rollup._streaming_writer(out, schema):
        pass
    assert not out.exists()
    assert not out.with_suffix(".parquet.tmp").exists()


def test_streaming_writer_one_batch(tmp_path):
    out = tmp_path / "data.parquet"
    schema = _schema_for_spec(FakeRow, TableSpec("fakes"))
    with Rollup._streaming_writer(out, schema) as append:
        append(asdict(FakeRow(feed_timestamp=1, destination="A", direction="W")))
        append(asdict(FakeRow(feed_timestamp=2, destination="B", direction="E")))

    table = pq.ParquetFile(out).read()
    assert table.num_rows == 2
    assert table.column("destination").to_pylist() == ["A", "B"]


def test_streaming_writer_multiple_batches(tmp_path):
    out = tmp_path / "data.parquet"
    schema = _schema_for_spec(FakeRow, TableSpec("fakes"))
    with Rollup._streaming_writer(out, schema, batch_size=3) as append:
        for i in range(7):  # forces 2 flushes + final
            append(
                asdict(FakeRow(feed_timestamp=i, destination=f"d{i}", direction="W"))
            )

    table = pq.ParquetFile(out).read()
    assert table.num_rows == 7


def test_streaming_writer_exception_no_orphan(tmp_path):
    out = tmp_path / "data.parquet"
    schema = _schema_for_spec(FakeRow, TableSpec("fakes"))
    with pytest.raises(RuntimeError):
        with Rollup._streaming_writer(out, schema) as append:
            append(asdict(FakeRow(feed_timestamp=1, destination="A", direction="W")))
            raise RuntimeError("boom")

    # final parquet shouldn't exist (atomic rename never happened)
    assert not out.exists()
    # tmp shouldn't be left behind either
    assert not out.with_suffix(".parquet.tmp").exists()


def test_streaming_writer_applies_column_renames(tmp_path):
    out = tmp_path / "data.parquet"
    spec = StandardDecoder.produces[VehicleRow]
    schema = _schema_for_spec(VehicleRow, spec)
    with Rollup._streaming_writer(
        out, schema, column_names=spec.column_names
    ) as append:
        append(asdict(VehicleRow(vehicle_id="v1", vehicle_timestamp=1700000000)))
        append(asdict(VehicleRow(vehicle_id="v2", vehicle_timestamp=1700000001)))

    table = pq.ParquetFile(out).read()
    assert table.column("vehicle.vehicle.id").to_pylist() == ["v1", "v2"]
    assert table.column("vehicle.timestamp").to_pylist() == [1700000000, 1700000001]
    # null-pad extras come through as all-null
    assert table.column("vehicle.trip.revenue").to_pylist() == [None, None]


def test_rollup_routes_unknwon_decoder_to_its_own_table(tmp_path):
    pass


def test_skip_happens_when_outputs_exist(tmp_path, monkeypatch):
    landing_dir = tmp_path / "landing"
    curated_dir = tmp_path / "curated"
    feed = Feed(
        name="fake-feed",
        path="/whatever",
        parser=None,
        decoder=FakeDecoder(),
        agency_id="A",
        poll_interval_seconds=60,
    )
    day = date(2026, 5, 1)
    rollup = Rollup(
        feeds=[feed], source=LocalSource(landing_dir), curated_dir=curated_dir
    )
    for path in rollup._expected_outputs(feed, day).values():
        path.parent.mkdir(parents=True, exist_ok=True)
        path.touch()
    metadata_calls = []
    data_calls = []
    monkeypatch.setattr(
        rollup, "_rollup_metadata", lambda *a, **kw: metadata_calls.append(1)
    )
    monkeypatch.setattr(rollup, "_rollup_data", lambda *a, **kw: data_calls.append(1))

    rollup.rollup_one("fake-feed", day)

    assert metadata_calls == []
    assert data_calls == []


def test_skip_does_not_happen_when_outputs_missing(tmp_path, monkeypatch):
    landing_dir = tmp_path / "landing"
    curated_dir = tmp_path / "curated"
    feed = Feed(
        name="fake-feed",
        path="/whatever",
        parser=None,
        decoder=FakeDecoder(),
        agency_id="A",
        poll_interval_seconds=60,
    )
    day = date(2026, 5, 1)
    rollup = Rollup(
        feeds=[feed], source=LocalSource(landing_dir), curated_dir=curated_dir
    )
    metadata_calls = []
    data_calls = []
    monkeypatch.setattr(
        rollup, "_rollup_metadata", lambda *a, **kw: metadata_calls.append(1)
    )
    monkeypatch.setattr(rollup, "_rollup_data", lambda *a, **kw: data_calls.append(1))

    rollup.rollup_one(feed_name="fake-feed", day=day)

    assert metadata_calls == [1]
    assert data_calls == [1]


def test_if_force_true_bypasses_skip(tmp_path, monkeypatch):
    landing_dir = tmp_path / "landing"
    curated_dir = tmp_path / "curated"
    feed = Feed(
        name="fake-feed",
        path="/whatever",
        parser=None,
        decoder=FakeDecoder(),
        agency_id="A",
        poll_interval_seconds=60,
    )
    day = date(2026, 5, 1)
    rollup = Rollup(
        feeds=[feed], source=LocalSource(landing_dir), curated_dir=curated_dir
    )
    for path in rollup._expected_outputs(feed, day).values():
        path.parent.mkdir(parents=True, exist_ok=True)
        path.touch()
    metadata_calls = []
    data_calls = []
    monkeypatch.setattr(
        rollup, "_rollup_metadata", lambda *a, **kw: metadata_calls.append(1)
    )
    monkeypatch.setattr(rollup, "_rollup_data", lambda *a, **kw: data_calls.append(1))

    rollup.rollup_one(feed_name="fake-feed", day=day, force=True)

    assert metadata_calls == [1]
    assert data_calls == [1]


def test_framed_window_rolls_up_with_joined_fetched_at(tmp_path):
    """A framed window yields one payload per frame, and each frame's fetched_at is
    recovered by joining its content digest to the metadata jsonl's timestamp."""
    landing_dir = tmp_path / "landing"
    curated_dir = tmp_path / "curated"
    day = date(2026, 5, 1)
    payloads = [b"alpha", b"bravo"]

    _write_framed_window(
        landing_dir, "echo-feed", day, window_start=900, payloads=payloads
    )
    _write_metadata(
        landing_dir,
        "echo-feed",
        day,
        rows=[
            {
                "timestamp": 1000,
                "status_code": 200,
                "digest": hashlib.sha256(b"alpha").hexdigest(),
            },
            {
                "timestamp": 2000,
                "status_code": 200,
                "digest": hashlib.sha256(b"bravo").hexdigest(),
            },
        ],
    )

    rollup = Rollup(
        feeds=[_echo_feed()], source=LocalSource(landing_dir), curated_dir=curated_dir
    )
    rollup.rollup_one("echo-feed", day, force=True)

    out = (
        curated_dir
        / "echoes"
        / "feed=echo-feed"
        / "year=2026"
        / "month=5"
        / "day=1"
        / "data.parquet"
    )
    table = pq.ParquetFile(out).read()
    got = sorted(
        zip(table.column("payload").to_pylist(), table.column("fetched_at").to_pylist())
    )
    # Both frames parsed individually, each carrying its own joined per-poll timestamp.
    assert got == [("alpha", 1000), ("bravo", 2000)]


def test_framed_window_unknown_digest_falls_back_to_window_start(tmp_path):
    """A frame whose digest is absent from the metadata index falls back to the
    window-start unix in the filename rather than crashing or dropping the row."""
    landing_dir = tmp_path / "landing"
    curated_dir = tmp_path / "curated"
    day = date(2026, 5, 1)

    _write_framed_window(
        landing_dir, "echo-feed", day, window_start=900, payloads=[b"orphan"]
    )
    # Index exists but holds an unrelated digest, so the orphan frame can't be joined.
    _write_metadata(
        landing_dir,
        "echo-feed",
        day,
        rows=[
            {
                "timestamp": 1000,
                "status_code": 304,
                "digest": hashlib.sha256(b"other").hexdigest(),
            }
        ],
    )

    rollup = Rollup(
        feeds=[_echo_feed()], source=LocalSource(landing_dir), curated_dir=curated_dir
    )
    rollup.rollup_one("echo-feed", day, force=True)

    out = (
        curated_dir
        / "echoes"
        / "feed=echo-feed"
        / "year=2026"
        / "month=5"
        / "day=1"
        / "data.parquet"
    )
    table = pq.ParquetFile(out).read()
    assert table.column("payload").to_pylist() == ["orphan"]
    assert table.column("fetched_at").to_pylist() == [900]  # window-start fallback


def test_truncated_framed_window_keeps_complete_frames(tmp_path):
    """Truncation mid-window is tolerated: complete frames roll up, the rest is
    dropped, and the day does not crash (the FrameError/EOFError policy)."""
    landing_dir = tmp_path / "landing"
    curated_dir = tmp_path / "curated"
    day = date(2026, 5, 1)

    full = _framed_bytes([b"alpha", b"bravo"])
    truncated = full[:-3]  # lop off the tail of the second frame's payload
    raw_path = (
        landing_dir
        / "echo-feed"
        / "raw"
        / "year=2026"
        / "month=5"
        / "day=1"
        / "window=900.bin"
    )
    raw_path.parent.mkdir(parents=True, exist_ok=True)
    raw_path.write_bytes(truncated)
    _write_metadata(
        landing_dir,
        "echo-feed",
        day,
        rows=[
            {
                "timestamp": 1000,
                "status_code": 200,
                "digest": hashlib.sha256(b"alpha").hexdigest(),
            }
        ],
    )

    rollup = Rollup(
        feeds=[_echo_feed()], source=LocalSource(landing_dir), curated_dir=curated_dir
    )
    rollup.rollup_one("echo-feed", day, force=True)  # must not raise

    out = (
        curated_dir
        / "echoes"
        / "feed=echo-feed"
        / "year=2026"
        / "month=5"
        / "day=1"
        / "data.parquet"
    )
    table = pq.ParquetFile(out).read()
    assert table.column("payload").to_pylist() == ["alpha"]  # only the intact frame


def test_digest_timestamps_skips_digestless_rows_silently(tmp_path, caplog):
    """Transport-error rows carry a timestamp but no digest (nothing was stored). They
    must be dropped from the join map silently — not warned about (a feed outage would
    otherwise spam WARNING) and not crash the build."""
    import logging

    landing_dir = tmp_path / "landing"
    day = date(2026, 5, 1)
    _write_metadata(
        landing_dir,
        "echo-feed",
        day,
        rows=[
            {"timestamp": 1000, "status_code": 200, "digest": "abc"},
            {
                "timestamp": 1001,
                "error_type": "ConnectionError",
                "error_message": "boom",
            },  # no digest
            {"timestamp": 1002, "status_code": 304, "digest": "def"},
        ],
    )
    source = LocalSource(landing_dir)

    with caplog.at_level(logging.WARNING):
        got = digest_timestamps(source, "echo-feed", day)

    assert got == {"abc": 1000, "def": 1002}  # digest-less row dropped, others kept
    assert "missing" not in caplog.text.lower()  # no spurious warning emitted


def test_hourly_merged_bin_rolls_up_correctly(tmp_path):
    """hour=*.bin files (produced by LandingUploader with merge_to_hourly=True) are
    framed files — the rollup must treat them the same as window=*.bin, not crash
    trying to parse 'hour=...' as a legacy numeric timestamp."""
    import hashlib

    from archiver.writer import merge_bins

    landing_dir = tmp_path / "landing"
    curated_dir = tmp_path / "curated"
    day = date(2026, 6, 25)

    # Build two window .bin files the uploader would have merged.
    w1 = _framed_bytes([b"alpha"])
    w2 = _framed_bytes([b"bravo"])

    # Write them as temp window files so merge_bins can read them.
    raw_dir = (
        landing_dir
        / "echo-feed"
        / "raw"
        / f"year={day.year}"
        / f"month={day.month}"
        / f"day={day.day}"
    )
    raw_dir.mkdir(parents=True, exist_ok=True)
    w1_path = raw_dir / "window=1750809600.bin"
    w2_path = raw_dir / "window=1750811400.bin"
    w1_path.write_bytes(w1)
    w2_path.write_bytes(w2)

    # Merge into an hour=*.bin file (mimics LandingUploader._merge_and_ship).
    merged = merge_bins([w1_path, w2_path])
    hour_path = raw_dir / "hour=1750809600.bin"
    hour_path.write_bytes(merged)
    w1_path.unlink()
    w2_path.unlink()

    _write_metadata(
        landing_dir,
        "echo-feed",
        day,
        rows=[
            {
                "timestamp": 1750809601,
                "status_code": 200,
                "digest": hashlib.sha256(b"alpha").hexdigest(),
            },
            {
                "timestamp": 1750811401,
                "status_code": 200,
                "digest": hashlib.sha256(b"bravo").hexdigest(),
            },
        ],
    )

    rollup = Rollup(
        feeds=[_echo_feed()], source=LocalSource(landing_dir), curated_dir=curated_dir
    )
    rollup.rollup_one("echo-feed", day, force=True)  # must not raise

    out = (
        curated_dir
        / "echoes"
        / "feed=echo-feed"
        / f"year={day.year}"
        / f"month={day.month}"
        / f"day={day.day}"
        / "data.parquet"
    )
    table = pq.ParquetFile(out).read()
    assert sorted(table.column("payload").to_pylist()) == ["alpha", "bravo"]
    assert table.column("fetched_at").to_pylist() == [1750809601, 1750811401]


def test_second_run_does_not_redo_work(tmp_path):
    landing_dir = tmp_path / "landing"
    curated_dir = tmp_path / "curated"
    metadata_path = (
        landing_dir
        / "fake-feed"
        / "metadata"
        / "year=2026"
        / "month=5"
        / "day=1"
        / "data.jsonl"
    )
    metadata_path.parent.mkdir(parents=True, exist_ok=True)
    with open(metadata_path, "a") as f:
        f.write('{"status_code": 200, "fetched_at": 1234567890}\n')
    raw_path = (
        landing_dir
        / "fake-feed"
        / "raw"
        / "year=2026"
        / "month=5"
        / "day=1"
        / "1234567890.bin"
    )
    raw_path.parent.mkdir(parents=True, exist_ok=True)
    with open(raw_path, "wb") as f:
        f.write(b"\x00\x01\x02\x03")
    feed = Feed(
        name="fake-feed",
        path="/whatever",
        parser=FakeParser(),
        decoder=FakeDecoder(),
        agency_id="A",
        poll_interval_seconds=60,
    )
    day = date(2026, 5, 1)
    rollup = Rollup(
        feeds=[feed], source=LocalSource(landing_dir), curated_dir=curated_dir
    )

    rollup.run(feed="fake-feed", day=day)  # first run — produces parquet

    # Snapshot every output file's bytes
    snapshot = {p: p.read_bytes() for p in (tmp_path / "curated").rglob("*.parquet")}
    assert snapshot, "first run produced no outputs"  # sanity check

    rollup.run(feed="fake-feed", day=day)  # second run — should skip

    # Bytes should be unchanged
    for path, original_bytes in snapshot.items():
        assert path.read_bytes() == original_bytes, f"{path} was rewritten"


def test_rollup_uses_rust_decoder_for_standard_decoder_feeds(tmp_path):
    """Covers the rust branch of _rollup_data, which no other test reaches.

    Every other test here uses FakeDecoder/EchoDecoder, so
    `type(feed.decoder) is StandardDecoder` is False and rollup.py:133 never
    takes the rail_decoder path. This one uses a real StandardDecoder instance
    so the branch actually runs.
    """
    landing_dir, curated_dir = tmp_path / "landing", tmp_path / "curated"
    day = date(2026, 5, 1)

    # Reuse the parity fixtures rather than inventing payloads -- same bytes the
    # golden files were generated from, so row counts are checkable against them.
    fixture = Path("tests/fixtures/golden/nyct-l")
    payloads = json.loads((fixture / "payloads.json").read_text())

    # Legacy LocalWriter .bin shape: stem IS the wall-clock unix ts and the whole
    # file is ONE payload (payloads.py:120). No \x89GRT framing, and no metadata
    # jsonl -- digest_timestamps returns {} when metadata is absent, which the
    # legacy branch never consults anyway.
    raw = (
        landing_dir
        / "nyct-l"
        / "raw"
        / f"year={day.year}/month={day.month}/day={day.day}"
    )
    raw.mkdir(parents=True)
    for p in payloads:
        (raw / f"{int(p['fetched_at'])}.bin").write_bytes(
            base64.b64decode(p["payload"])
        )

    feed = Feed(
        name="nyct-l",
        path="/whatever",
        parser=None,  # unused on the rust path -- it decodes bytes directly
        decoder=StandardDecoder(),  # <- the whole point; an *instance*, not the class
        agency_id="MTA",
        poll_interval_seconds=60,
    )
    rollup = Rollup(
        feeds=[feed], source=LocalSource(landing_dir), curated_dir=curated_dir
    )

    rollup._rollup_data(feed, day)  # direct, to skip the _expected_outputs gate

    # TODO: assert the parquet landed and the row counts match the goldens --
    # 291 vehicles, 6950 trip_updates. Read the golden JSON lengths rather than
    # hardcoding, so regenerating fixtures doesn't silently desync the test.


def test_iter_payloads_hash_named_joins_digest_to_timestamp():
    """A digest-named file is one unframed payload, dated only by the join."""
    data = b"alpha"
    digest = hashlib.sha256(data).hexdigest()

    got = list(iter_payloads(f"{digest}.bin", data, {digest: 1700000000}))

    assert got == [(data, 1700000000)]


def test_iter_payloads_hash_named_without_metadata_row_drops_loudly(caplog):
    """No join, no fallback: the stem carries no time, so the row is dropped.

    Dropping is the deliberate choice over fabricating a timestamp — a made-up
    fetched_at would file the payload under the wrong day and stay plausible.
    The warning is what makes the loss detectable, so it is asserted too.
    """
    data = b"orphan"
    digest = hashlib.sha256(data).hexdigest()

    with caplog.at_level(logging.WARNING):
        got = list(iter_payloads(f"{digest}.bin", data, {}))

    assert got == []
    warnings = [r for r in caplog.records if r.levelno == logging.WARNING]
    assert len(warnings) == 1
    assert digest in caplog.text
    assert "dropping" in caplog.text.lower()


def test_iter_payloads_all_digit_digest_takes_hash_branch_not_legacy():
    """The branch-ordering hazard: a digest that is also a valid numeral.

    "1" * 64 is a syntactically valid sha256 hexdigest AND parses as a float,
    so the two branches genuinely overlap on it. _HASH_STEM must be tested
    before the legacy int(float(stem)) fallback — the legacy parse does not
    raise here, it silently returns ~1.1e63, which would be accepted as a
    fetched_at and partition the payload into a nonexistent year.
    """
    stem = "1" * 64
    data = b"payload"

    got = list(iter_payloads(f"{stem}.bin", data, {stem: 1700000000}))

    assert got == [(data, 1700000000)]

    # what the legacy branch would have produced instead: no exception to catch,
    # just a number far outside any plausible unix timestamp
    assert int(float(stem)) > 10**60
