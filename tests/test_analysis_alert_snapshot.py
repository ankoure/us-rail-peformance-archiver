import hashlib
import io
import json
from datetime import date

from google.transit.gtfs_realtime_pb2 import FeedHeader, FeedMessage

from analysis.alert_snapshot import build_alert_snapshot
from archiver.feed import Feed
from archiver.parser import Parser
from archiver.source import LocalSource
from archiver.writer import FrameWriter


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
    with path.open("a") as f:
        for row in rows:
            f.write(json.dumps(row) + "\n")
    return path


def _write_window(
    landing_dir,
    feed_name: str,
    day: date,
    window_start: int,
    polls: list[tuple[bytes, int]],
):
    """One BatchingWriter-style framed window file holding `polls`, each a
    (payload, true_fetched_at) pair -- paired with the metadata jsonl rows
    that let digest_timestamps recover each frame's real timestamp (rather
    than falling back to the coarse window_start for every frame in the
    file, which is what happens with no metadata at all). Mirrors real
    landing output, where every stored frame has a metadata row."""
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
    buf = io.BytesIO()
    writer = FrameWriter(buf)
    rows = []
    for payload, fetched_at in polls:
        digest = hashlib.sha256(payload).digest()
        writer.write_frame(payload, digest)
        rows.append({"digest": digest.hex(), "timestamp": fetched_at})
    path.write_bytes(buf.getvalue())
    _write_metadata(landing_dir, feed_name, day, rows)
    return path


def _feed_message(
    *, timestamp: int, alerts: dict[str, str] | None = None, vehicle_ids: list[str] = ()
) -> bytes:
    """A FeedMessage with a service alert per (id, header_text) pair in
    `alerts`, plus a bare vehicle-position entity per id in `vehicle_ids` --
    the combined-feed shape (BKK/EDMONTON/VBB) that made build_alert_snapshot
    buffer a whole day of mostly-irrelevant vehicle/trip data just to find the
    rare alert entities mixed in."""
    feed = FeedMessage()
    feed.header.gtfs_realtime_version = "2.0"
    feed.header.incrementality = FeedHeader.FULL_DATASET
    feed.header.timestamp = timestamp
    for alert_id, header_text in (alerts or {}).items():
        entity = feed.entity.add()
        entity.id = alert_id
        translation = entity.alert.header_text.translation.add()
        translation.text = header_text
        translation.language = "en"
    for vid in vehicle_ids:
        entity = feed.entity.add()
        entity.id = f"vehicle-{vid}"
        entity.vehicle.vehicle.id = vid
    return feed.SerializeToString()


def _feed(name: str = "test-feed") -> Feed:
    return Feed(
        name=name,
        path="/irrelevant",
        parser=Parser.from_name("protobuf"),
        decoder=None,
        agency_id="TEST_AGENCY",
    )


def test_last_write_wins_by_fetched_at_not_file_order(tmp_path):
    """Two polls carry the same alert id with different bodies. The later
    poll's body must win regardless of which .bin file iter_bins happens to
    yield first -- iter_bins order isn't guaranteed, only fetched_at is
    trustworthy, which is exactly what the docstring's re-sort exists for."""
    day = date(2026, 1, 15)
    feed = _feed()

    # Write the LATER poll to disk first and the earlier poll second, so a
    # naive "trust file iteration order" implementation would get this backwards.
    _write_window(
        tmp_path,
        feed.name,
        day,
        window_start=2000,
        polls=[
            (_feed_message(timestamp=2000, alerts={"alert-1": "second body"}), 2000)
        ],
    )
    _write_window(
        tmp_path,
        feed.name,
        day,
        window_start=1000,
        polls=[(_feed_message(timestamp=1000, alerts={"alert-1": "first body"}), 1000)],
    )

    snapshot = build_alert_snapshot(feed, day, LocalSource(tmp_path))

    assert set(snapshot["alerts"]) == {"alert-1"}
    entry = snapshot["alerts"]["alert-1"]
    assert entry["alert"]["header_text"]["translation"][0]["text"] == "second body"
    assert entry["first_seen"] == 1000
    assert entry["last_seen"] == 2000
    assert entry["poll_count"] == 2


def test_vehicle_only_polls_do_not_pollute_alerts_but_still_update_header(tmp_path):
    """A combined feed's polls are mostly vehicle positions with no alerts at
    all -- those polls must not add spurious alert entries, but the feed
    header (used for feed_header in the snapshot) still reflects the latest
    poll seen, alert or not."""
    day = date(2026, 1, 15)
    feed = _feed()

    _write_window(
        tmp_path,
        feed.name,
        day,
        window_start=1000,
        polls=[(_feed_message(timestamp=1000, alerts={"alert-1": "body"}), 1000)],
    )
    _write_window(
        tmp_path,
        feed.name,
        day,
        window_start=2000,
        polls=[(_feed_message(timestamp=2000, vehicle_ids=["bus-1", "bus-2"]), 2000)],
    )

    snapshot = build_alert_snapshot(feed, day, LocalSource(tmp_path))

    assert set(snapshot["alerts"]) == {"alert-1"}
    assert snapshot["alerts"]["alert-1"]["poll_count"] == 1
    assert snapshot["feed_header"]["timestamp"] == "2000"


def test_multiple_alerts_and_a_multi_poll_window(tmp_path):
    """A single window file can hold several framed polls (BatchingWriter's
    normal shape); each must still be folded independently by its OWN true
    (metadata-joined) fetched_at, and distinct alert ids must not clobber
    each other."""
    day = date(2026, 1, 15)
    feed = _feed()

    _write_window(
        tmp_path,
        feed.name,
        day,
        window_start=1000,
        polls=[
            (
                _feed_message(
                    timestamp=1000, alerts={"alert-1": "a1v1", "alert-2": "a2v1"}
                ),
                1000,
            ),
            (_feed_message(timestamp=1030, alerts={"alert-1": "a1v2"}), 1030),
        ],
    )

    snapshot = build_alert_snapshot(feed, day, LocalSource(tmp_path))

    assert (
        snapshot["alerts"]["alert-1"]["alert"]["header_text"]["translation"][0]["text"]
        == "a1v2"
    )
    assert snapshot["alerts"]["alert-1"]["first_seen"] == 1000
    assert snapshot["alerts"]["alert-1"]["last_seen"] == 1030
    assert snapshot["alerts"]["alert-1"]["poll_count"] == 2
    assert snapshot["alerts"]["alert-2"]["poll_count"] == 1


def test_no_polls_for_day_yields_empty_snapshot(tmp_path):
    day = date(2026, 1, 15)
    feed = _feed()
    snapshot = build_alert_snapshot(feed, day, LocalSource(tmp_path))
    assert snapshot["alerts"] == {}
    assert snapshot["feed_header"] is None
