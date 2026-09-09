"""Unit tests for archiver.landing_uploader.LandingUploader.

No real S3, no real telemetry: Uploader and Telemetry are replaced with
in-memory fakes, and everything runs against a tmp_path landing dir.

Most tests drive the internals (_pending / _ship_one / _scan_once_window)
synchronously so nothing depends on thread timing; the lifecycle tests at the
bottom exercise the real worker thread through __aenter__/__aexit__.
"""

from __future__ import annotations

import asyncio
import logging
import os
import threading
import time
from pathlib import Path

import pytest


from archiver import landing_uploader as mod
from archiver.landing_uploader import LandingUploader

import hashlib
import json
from datetime import datetime, timezone


import io
import tarfile

from archiver.landing_layout import window_metadata_local_path, window_tar_key

NOW = datetime(2026, 5, 4, 12, 0, 0, tzinfo=timezone.utc).timestamp()
WINDOW = 300.0
GRACE = 60.0  # restates _WINDOW_GRACE_S on purpose: if the constant moves, the
# boundary test below should fail rather than silently slide with it.
FEED = "test-feed"


# --- fixtures -------------------------------------------------------------


def _day_dir(landing: Path, feed: str, kind: str, unix_ts: float) -> Path:
    """landing/feed/{kind}/year=Y/month=M/day=D for a UTC timestamp.

    Month and day unpadded, matching what BaseWriter.append_metadata and
    ContentAddressedWriter._write_sync actually write. Deliberately NOT
    LandingUploader._day_partition: if the fixture called production's
    partitioner the layout would only ever be checked against itself, and the
    unpadded-month detail would stop being pinned by anything.
    """
    d = datetime.fromtimestamp(unix_ts, tz=timezone.utc)
    return (
        landing / feed / kind / f"year={d.year}" / f"month={d.month}" / f"day={d.day}"
    )


def add_metadata_row(
    landing: Path, feed: str, timestamp: float, digest: str | None, **extra
) -> dict:
    """Append one row to the day's data.jsonl. digest=None models a 304."""
    path = _day_dir(landing, feed, "metadata", timestamp) / "data.jsonl"
    path.parent.mkdir(parents=True, exist_ok=True)
    row = {"timestamp": timestamp, "digest": digest, **extra}
    with path.open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(row) + "\n")
    return row


def add_raw_bin(
    landing: Path, feed: str, timestamp: float, payload: bytes = b"x"
) -> str:
    """Write {sha256(payload)}.bin under the day's raw dir. Returns the digest.

    The digest is derived, never passed in, so a fixture cannot create the
    digest/payload mismatch that ContentAddressedWriter makes impossible.
    """
    digest = hashlib.sha256(payload).hexdigest()
    path = _day_dir(landing, feed, "raw", timestamp) / f"{digest}.bin"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(payload)
    return digest


def add_response(
    landing: Path, feed: str, timestamp: float, payload: bytes = b"x", **extra
) -> str:
    """A bin plus the metadata row pointing at it — the common case."""
    digest = add_raw_bin(landing, feed, timestamp, payload)
    add_metadata_row(landing, feed, timestamp, digest, **extra)
    return digest


def tar_members(blob: bytes) -> list[tuple[str, bytes]]:
    """[(arcname, contents)] for an uploaded tar, in archive order.

    A list, not a dict: tar permits repeated arcnames, so keying by name would
    silently collapse a duplicate member and hide a dedup regression.
    """
    with tarfile.open(fileobj=io.BytesIO(blob)) as tar:
        return [(m.name, tar.extractfile(m).read()) for m in tar.getmembers()]


class FrozenClock:
    """Stands in for the `time` module inside landing_uploader.

    Patched over the module's own `time` name rather than over `time.time`
    itself, so the stdlib module is never mutated for other threads mid-test.
    Only `.time()` is needed: it is the module's sole use of the import.
    """

    def __init__(self, now: float) -> None:
        self.now = now

    def time(self) -> float:
        return self.now


def capture_tars(up) -> list[bytes]:
    """Snapshot each uploaded file's bytes at PUT time. Call before shipping.

    Necessary because _ship_raw_window stages the tar inside a
    TemporaryDirectory that is torn down the moment the upload returns — so the
    Path recorded by FakeUploader.calls is already dangling by the time any
    assertion runs. Wraps rather than replaces upload(), so `up.calls` still
    records normally and the bucket/key assertions are unaffected.
    """
    blobs: list[bytes] = []
    real_upload = up.upload

    def capturing_upload(bucket, key, path):
        blobs.append(Path(path).read_bytes())
        return real_upload(bucket, key, path)

    up.upload = capturing_upload
    return blobs


def ship_ready(lu, now: float = NOW) -> None:
    """Run the raw path exactly as _maybe_ship_raw_windows does."""
    for feed, window_start, rows in lu._ready_raw_windows(now):
        lu._ship_raw_window(feed, window_start, rows)


# --- fakes -------------------------------------------------------------------


class FakeUploader:
    """Records uploads; can fail per-key with a configurable exception."""

    def __init__(self) -> None:
        self.calls: list[tuple[str, str, Path]] = []  # (bucket, key, path)
        self.bodies: dict[str, bytes] = {}  # key -> file contents at upload time
        self.fail: dict[str, Exception] = {}  # key -> exception to raise
        self.block: threading.Event | None = None  # if set, upload() waits on it

    def upload(self, bucket: str, key: str, path: Path) -> None:
        if self.block is not None:
            self.block.wait()
        # Read NOW, not at assertion time: the hourly merge stages its output in
        # a TemporaryDirectory that is removed before _merge_and_ship returns,
        # so the recorded path is dangling by the time a test looks at it.
        # Reading here also means a path that was never written fails loudly in
        # the fake instead of passing silently.
        try:
            self.bodies[key] = Path(path).read_bytes()
        except OSError:
            pass
        self.calls.append((bucket, key, Path(path)))
        if key in self.fail:
            raise self.fail[key]

    @property
    def keys(self) -> list[str]:
        return [k for _, k, _ in self.calls]


class FakeTelemetry:
    def __init__(self) -> None:
        self.gauges: list[tuple[str, float]] = []

    def gauge(self, name: str, value: float) -> None:
        self.gauges.append((name, value))


# --- helpers -----------------------------------------------------------------


def make_uploader(
    tmp_path: Path, **kw
) -> tuple[LandingUploader, FakeUploader, FakeTelemetry]:
    up = FakeUploader()
    tel = FakeTelemetry()
    lu = LandingUploader(
        tmp_path,
        up,
        "test-bucket",
        kw.pop("prefix", "archive/"),
        telemetry=tel,
        **kw,
    )
    return lu, up, tel


def window_file(
    landing: Path,
    feed: str = "feedA",
    kind: str = "raw",
    name: str = "window=0001.jsonl",
    mtime: float | None = None,
) -> Path:
    """Create a window object at the exact depth writer.py uses."""
    p = landing / feed / kind / "year=2026" / "month=06" / "day=11" / name
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_bytes(b"x")
    if mtime is not None:
        os.utime(p, (mtime, mtime))
    return p


def wait_until(cond, timeout: float = 5.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if cond():
            return
        time.sleep(0.01)
    raise AssertionError("condition not met within timeout")


# --- construction ------------------------------------------------------------


def test_prefix_without_trailing_slash_is_rejected(tmp_path):
    with pytest.raises(ValueError, match="must end with '/'"):
        LandingUploader(
            tmp_path, FakeUploader(), "b", "data", telemetry=FakeTelemetry()
        )


@pytest.mark.parametrize("prefix", ["", "data/", "a/b/"])
def test_valid_prefixes_are_accepted(tmp_path, prefix):
    LandingUploader(tmp_path, FakeUploader(), "b", prefix, telemetry=FakeTelemetry())


# --- key mapping ---------------------------------------------------------------


def test_key_is_prefix_plus_relative_posix_path(tmp_path):
    lu, _, _ = make_uploader(tmp_path, prefix="archive/")
    p = window_file(tmp_path)
    assert (
        lu._key_for(p)
        == "archive/feedA/raw/year=2026/month=06/day=11/window=0001.jsonl"
    )


def test_key_with_empty_prefix_is_bare_relative_path(tmp_path):
    lu, _, _ = make_uploader(tmp_path, prefix="")
    p = window_file(tmp_path, kind="metadata", name="window=0001.jsonl")
    assert (
        lu._key_for(p) == "feedA/metadata/year=2026/month=06/day=11/window=0001.jsonl"
    )


# --- _pending: selection ----------------------------------------------------


def test_pending_matches_metadata_only(tmp_path):
    """Raw is no longer glob-discoverable -- only metadata window objects are."""
    lu, _, _ = make_uploader(tmp_path)
    b = window_file(tmp_path, kind="metadata", name="window=0001.jsonl")
    raw = window_file(tmp_path, kind="raw", name="window=0001.bin")
    assert set(lu._pending()) == {b}
    assert raw not in lu._pending()


def test_pending_skips_local_only_files(tmp_path):
    """data.jsonl, *.tmp, wrong-suffix and wrong-depth files never ship."""
    lu, _, _ = make_uploader(tmp_path)
    day = tmp_path / "feedA" / "raw" / "year=2026" / "month=06" / "day=11"
    day.mkdir(parents=True)
    (day / "data.jsonl").write_bytes(b"x")  # local-only db
    (day / "window=0002.tmp").write_bytes(b"x")  # in-progress write
    (day / "window=0003.jsonl").write_bytes(b"x")  # wrong suffix under raw/
    (tmp_path / "window=stray.jsonl").write_bytes(b"x")  # wrong depth
    assert lu._pending() == []


def test_pending_skips_directories_matching_the_glob(tmp_path):
    lu, _, _ = make_uploader(tmp_path)
    d = (
        tmp_path
        / "feedA"
        / "raw"
        / "year=2026"
        / "month=06"
        / "day=11"
        / "window=0001.jsonl"
    )
    d.mkdir(parents=True)  # a *directory* with a matching name
    assert lu._pending() == []


def test_pending_orders_by_mtime(tmp_path):
    """Ordering is by mtime, not by name."""
    lu, _, _ = make_uploader(tmp_path)
    newest = window_file(
        tmp_path, kind="metadata", name="window=0001.jsonl", mtime=3000
    )
    oldest = window_file(
        tmp_path, kind="metadata", name="window=0009.jsonl", mtime=1000
    )
    middle = window_file(
        tmp_path, kind="metadata", name="window=0005.jsonl", mtime=2000
    )
    assert lu._pending() == [oldest, middle, newest]


def test_pending_sorts_vanished_files_last(tmp_path, monkeypatch):
    lu, _, _ = make_uploader(tmp_path)
    gone = window_file(tmp_path, kind="metadata", name="window=0001.jsonl", mtime=1000)
    stays = window_file(tmp_path, kind="metadata", name="window=0002.jsonl", mtime=2000)

    real_stat = Path.stat
    seen = {"count": 0}

    def stat(self, *a, **kw):
        if self == gone:
            seen["count"] += 1
            if seen["count"] > 1:  # 1st call is glob's is_file(); fail after
                raise OSError("pruned mid-scan")
        return real_stat(self, *a, **kw)

    monkeypatch.setattr(Path, "stat", stat)
    # `gone` is older but its stat fails -> it sorts to the end, not the front.
    assert lu._pending() == [stays, gone]


# --- _ship_one ---------------------------------------------------------------


def test_ship_one_uploads_then_deletes(tmp_path):
    lu, up, _ = make_uploader(tmp_path, prefix="archive/")
    p = window_file(tmp_path)
    lu._ship_one(p)
    assert up.calls == [
        (
            "test-bucket",
            "archive/feedA/raw/year=2026/month=06/day=11/window=0001.jsonl",
            p,
        )
    ]
    assert not p.exists()


def test_ship_one_keeps_file_on_upload_failure(tmp_path, caplog):
    lu, up, _ = make_uploader(tmp_path)
    p = window_file(tmp_path)
    up.fail[lu._key_for(p)] = RuntimeError("S3 down")
    with caplog.at_level(logging.ERROR):
        lu._ship_one(p)  # must not raise
    assert p.exists()  # retried next scan
    assert any("will retry next scan" in r.message for r in caplog.records)


def test_ship_one_treats_file_not_found_as_benign(tmp_path, caplog):
    """File pruned between scan and ship: no log noise, nothing raised."""
    lu, up, _ = make_uploader(tmp_path)
    p = window_file(tmp_path)
    up.fail[lu._key_for(p)] = FileNotFoundError(str(p))
    with caplog.at_level(logging.WARNING):
        lu._ship_one(p)
    assert caplog.records == []


# --- _scan_once_window --------------------------------------------------------------


def test_scan_once_window_ships_everything_oldest_first(tmp_path):
    lu, up, _ = make_uploader(tmp_path, prefix="")
    window_file(tmp_path, kind="metadata", name="window=0002.jsonl", mtime=2000)
    window_file(tmp_path, kind="metadata", name="window=0001.jsonl", mtime=1000)
    lu._scan_once_window()
    assert up.keys == [
        "feedA/metadata/year=2026/month=06/day=11/window=0001.jsonl",
        "feedA/metadata/year=2026/month=06/day=11/window=0002.jsonl",
    ]
    assert lu._pending() == []  # all deleted


def test_scan_once_window_emits_pending_gauge_each_pass(tmp_path):
    lu, _, tel = make_uploader(tmp_path)
    window_file(tmp_path, kind="metadata", name="window=0001.jsonl")
    window_file(tmp_path, kind="metadata", name="window=0002.jsonl")
    lu._scan_once_window()
    lu._scan_once_window()
    assert tel.gauges == [("landing.pending", 2), ("landing.pending", 0)]


def test_one_bad_object_does_not_starve_the_rest(tmp_path):
    lu, up, _ = make_uploader(tmp_path, prefix="")
    bad = window_file(tmp_path, kind="metadata", name="window=0001.jsonl", mtime=1000)
    good = window_file(tmp_path, kind="metadata", name="window=0002.jsonl", mtime=2000)
    up.fail[lu._key_for(bad)] = RuntimeError("corrupt")
    lu._scan_once_window()
    assert bad.exists() and not good.exists()
    # Failed object is retried on the next scan once the failure clears.
    up.fail.clear()
    lu._scan_once_window()
    assert not bad.exists()


def test_scan_once_window_bails_between_objects_on_stop(tmp_path):
    lu, up, _ = make_uploader(tmp_path)
    window_file(tmp_path, kind="metadata", name="window=0001.jsonl")
    window_file(tmp_path, kind="metadata", name="window=0002.jsonl")
    lu._stop.set()
    lu._scan_once_window()
    assert up.calls == []  # stop checked before each ship


def test_scan_once_window_ignore_stop_runs_full_pass(tmp_path):
    """The final shutdown drain ships even though _stop is already set."""
    lu, up, _ = make_uploader(tmp_path)
    window_file(tmp_path, kind="metadata", name="window=0001.jsonl")
    window_file(tmp_path, kind="metadata", name="window=0002.jsonl")
    lu._stop.set()
    lu._scan_once_window(ignore_stop=True)
    assert len(up.calls) == 2


# --- layout-mismatch sentinel --------------------------------------------------


def test_layout_mismatch_logs_when_stray_window_file_matches_no_glob(tmp_path, caplog):
    lu, _, _ = make_uploader(tmp_path)
    # writer.py "drifted": one extra path level, so the pinned globs miss it.
    p = (
        tmp_path
        / "feedA"
        / "raw"
        / "v2"
        / "year=2026"
        / "month=06"
        / "day=11"
        / "window=0001.jsonl"
    )
    p.parent.mkdir(parents=True)
    p.write_bytes(b"x")
    with caplog.at_level(logging.ERROR):
        assert lu._pending() == []
    assert any("layout mismatch" in r.message for r in caplog.records)


def test_layout_check_ignores_tmp_files(tmp_path, caplog):
    lu, _, _ = make_uploader(tmp_path)
    window_file(tmp_path, name="window=0001.tmp")  # in-progress write, not a stray
    with caplog.at_level(logging.ERROR):
        lu._pending()
    assert caplog.records == []


def test_layout_check_disarms_after_first_match(tmp_path, caplog):
    lu, up, _ = make_uploader(tmp_path)
    p = window_file(
        tmp_path, kind="metadata", name="window=0001.jsonl"
    )  # first scan matches -> check disarms forever
    lu._scan_once_window()
    assert not p.exists()
    # Now plant a stray: a disarmed check must stay silent on later empty scans.
    stray = tmp_path / "feedA" / "raw" / "extra" / "window=0009.jsonl"
    stray.parent.mkdir(parents=True)
    stray.write_bytes(b"x")
    with caplog.at_level(logging.ERROR):
        lu._scan_once_window()
    assert not any("layout mismatch" in r.message for r in caplog.records)


def test_layout_check_stays_armed_across_empty_scans(tmp_path, caplog):
    lu, _, _ = make_uploader(tmp_path)
    lu._scan_once_window()  # empty, no strays: silent but still armed
    p = tmp_path / "feedA" / "raw" / "extra" / "window=0009.jsonl"
    p.parent.mkdir(parents=True)
    p.write_bytes(b"x")
    with caplog.at_level(logging.ERROR):
        lu._scan_once_window()
    assert any("layout mismatch" in r.message for r in caplog.records)


# --- lifecycle (real worker thread) -------------------------------------------


def test_enter_triggers_boot_recovery_scan(tmp_path):
    """Files left over from a previous run ship without waiting an interval."""
    leftover = window_file(tmp_path, kind="metadata", name="window=0001.jsonl")

    async def go():
        lu, up, _ = make_uploader(tmp_path, scan_interval=60.0)
        async with lu:
            await asyncio.to_thread(wait_until, lambda: not leftover.exists())
        return up

    up = asyncio.run(go())
    assert len(up.calls) == 1


def test_exit_stops_thread_and_runs_final_drain(tmp_path):
    async def go():
        lu, up, tel = make_uploader(tmp_path, scan_interval=60.0)
        async with lu:
            # Wait for the worker's first (empty) scan, after which it parks
            # in its 60s wait. A file dropped now can only ship via the final
            # drain in __aexit__.
            await asyncio.to_thread(wait_until, lambda: len(tel.gauges) >= 1)
            window_file(tmp_path, kind="metadata", name="window=9999.jsonl")
        return lu, up

    lu, up = asyncio.run(go())
    assert not lu._thread.is_alive()
    assert any(k.endswith("window=9999.jsonl") for k in up.keys)
    assert lu._pending() == []


def test_exit_abandons_stuck_thread_after_timeout(tmp_path, monkeypatch, caplog):
    """A worker wedged in an upload must not hang shutdown, and the final
    drain must NOT run concurrently with it."""
    monkeypatch.setattr(mod, "_JOIN_TIMEOUT_S", 0.1)
    release = threading.Event()

    async def go():
        lu, up, _ = make_uploader(tmp_path, scan_interval=60.0)
        up.block = release  # wedge the worker inside upload()
        window_file(tmp_path)
        window_file(tmp_path, kind="metadata", name="window=0002.jsonl")
        async with lu:
            await asyncio.to_thread(wait_until, lambda: lu._thread.is_alive())
            # give the worker a moment to enter upload() and block
            await asyncio.sleep(0.1)
        return lu, up

    with caplog.at_level(logging.WARNING):
        lu, up = asyncio.run(go())
    assert any("did not stop" in r.message for r in caplog.records)
    assert up.calls == []  # final drain skipped: nothing shipped concurrently
    release.set()  # unwedge the daemon thread so it dies cleanly
    lu._thread.join(timeout=5.0)


def test_exit_swallows_final_drain_failure(tmp_path, monkeypatch, caplog):
    async def go():
        lu, _, _ = make_uploader(tmp_path, scan_interval=60.0)
        async with lu:
            await asyncio.sleep(0)  # enter/exit immediately
            monkeypatch.setattr(
                lu,
                "_scan_once_window",
                lambda *a, **kw: (_ for _ in ()).throw(RuntimeError()),
            )
        # reaching here at all = __aexit__ didn't propagate

    with caplog.at_level(logging.ERROR):
        asyncio.run(go())
    assert any("final landing drain failed" in r.message for r in caplog.records)


# --- _closed_windows / _ready_raw_windows ---------------------------------


def test_closed_windows_buckets_rows_by_window_start(tmp_path):
    """Rows group by floor(timestamp / window); the still-open window is out."""
    lu, _, _ = make_uploader(tmp_path, ship_window_seconds=WINDOW)
    w_old = int(NOW - 3 * WINDOW)
    w_mid = int(NOW - 2 * WINDOW)
    w_open = int(NOW - WINDOW)  # ends exactly at NOW, so grace hasn't elapsed

    first = add_response(tmp_path, FEED, w_old + 10, payload=b"one")
    # a 304 in the same window: no digest, but it still owns a metadata row
    add_metadata_row(tmp_path, FEED, w_old + 250, digest=None, status_code=304)
    second = add_response(tmp_path, FEED, w_mid + 5, payload=b"two")
    add_response(tmp_path, FEED, w_open + 150, payload=b"three")

    buckets = lu._closed_windows(FEED, NOW)

    assert sorted(buckets) == [w_old, w_mid]
    assert [row["digest"] for row in buckets[w_old]] == [first, None]
    assert [row["digest"] for row in buckets[w_mid]] == [second]


def test_grace_period_gates_a_just_closed_window(tmp_path):
    """A window is closed at end + grace, not at end.

    One on-disk state, three values of `now` — the window covers
    [NOW - WINDOW, NOW), so it ends exactly at NOW and becomes closed exactly
    GRACE seconds later.
    """
    lu, _, _ = make_uploader(tmp_path, ship_window_seconds=WINDOW)
    window_start = int(NOW - WINDOW)
    add_response(tmp_path, FEED, window_start + 150)

    assert lu._closed_windows(FEED, NOW) == {}
    assert lu._closed_windows(FEED, NOW + GRACE - 1) == {}
    assert sorted(lu._closed_windows(FEED, NOW + GRACE)) == [window_start]


def test_ready_windows_skips_a_window_with_a_shipped_marker(tmp_path):
    """The marker is what removes a window from the ready set — not the rows.

    data.jsonl is append-only, so the row is still there and still discoverable
    afterwards; the final assertion pins that, otherwise this test would pass
    just as well if the rows had been consumed.
    """
    lu, _, _ = make_uploader(tmp_path, ship_window_seconds=WINDOW)
    window_start = int(NOW - 3 * WINDOW)
    add_response(tmp_path, FEED, window_start + 10)

    assert [w for _, w, _ in lu._ready_raw_windows(NOW)] == [window_start]

    marker = tmp_path / FEED / ".shipped" / str(window_start)
    marker.parent.mkdir(parents=True, exist_ok=True)
    marker.touch()

    assert lu._ready_raw_windows(NOW) == []
    assert sorted(lu._closed_windows(FEED, NOW)) == [window_start]


# --- _ship_raw_window -----------------------------------------------------


def test_tar_holds_exactly_the_bins_still_present_on_disk(tmp_path):
    """Members are the window's present digests — absent ones are not an error.

    The third row points at a digest with no local file (content addressing
    collapsed it onto a copy an earlier window already shipped) and the fourth
    is a 304 with no digest at all. Neither belongs in the tar, and neither
    stops the window from shipping.
    """
    lu, up, _ = make_uploader(tmp_path, ship_window_seconds=WINDOW)
    ws = int(NOW - 3 * WINDOW)

    first = add_response(tmp_path, FEED, ws + 10, payload=b"alpha")
    second = add_response(tmp_path, FEED, ws + 20, payload=b"beta")
    add_metadata_row(
        tmp_path, FEED, ws + 30, digest=hashlib.sha256(b"gone").hexdigest()
    )
    add_metadata_row(tmp_path, FEED, ws + 40, digest=None, status_code=304)

    tars = capture_tars(up)
    ship_ready(lu)

    assert len(up.calls) == 1
    assert sorted(tar_members(tars[0])) == sorted(
        [(f"{first}.bin", b"alpha"), (f"{second}.bin", b"beta")]
    )

    # bins go only after a confirmed upload AND a materialized metadata slice
    assert list(_day_dir(tmp_path, FEED, "raw", ws).iterdir()) == []
    assert (tmp_path / FEED / ".shipped" / str(ws)).exists()


def test_repeated_digest_in_one_window_is_tarred_once(tmp_path):
    """Two rows sharing a digest reference one bin, so one member."""
    lu, up, _ = make_uploader(tmp_path, ship_window_seconds=WINDOW)
    ws = int(NOW - 3 * WINDOW)

    digest = add_response(tmp_path, FEED, ws + 10, payload=b"repeated")
    add_metadata_row(tmp_path, FEED, ws + 20, digest=digest)

    tars = capture_tars(up)
    ship_ready(lu)

    assert tar_members(tars[0]) == [(f"{digest}.bin", b"repeated")]

    # both rows still reach the metadata slice — dedup is bin-only
    lines = window_metadata_local_path(tmp_path, FEED, ws).read_text().splitlines()
    assert len(lines) == 2


def test_shipped_tar_gz_round_trips_through_s3source_iter_bins(tmp_path):
    """End-to-end: the real write path's bytes must be exactly what the real
    read path (S3Source.iter_bins) expects -- not just two test fixtures
    agreeing with each other. Guards the .tar.gz key/mode wiring between
    landing_uploader.py and source.py, which nothing enforces structurally --
    iter_bins matches on a literal ".tar.gz" suffix, not by calling
    window_tar_key, so a drift between the two would only surface here or in
    prod (silently, at next-day rollup, after the local bins are gone).
    """
    from archiver.source import S3Source

    lu, up, _ = make_uploader(tmp_path, ship_window_seconds=WINDOW)
    ws = int(NOW - 3 * WINDOW)

    first = add_response(tmp_path, FEED, ws + 10, payload=b"alpha")
    second = add_response(tmp_path, FEED, ws + 20, payload=b"beta")

    ship_ready(lu)

    assert len(up.calls) == 1
    bucket, key, _ = up.calls[0]
    assert key.endswith(".tar.gz")
    data = up.bodies[key]

    class OneKeyUploader:
        def list_keys(self, _bucket, prefix):
            return [key] if key.startswith(prefix) else []

        def get_bytes(self, _bucket, requested_key):
            assert requested_key == key
            return data

    day = datetime.fromtimestamp(ws, tz=timezone.utc).date()
    src = S3Source(OneKeyUploader(), bucket, prefix="archive/")

    assert dict(src.iter_bins(FEED, day)) == {
        f"{first}.bin": b"alpha",
        f"{second}.bin": b"beta",
    }


def test_all_304_window_ships_metadata_with_no_tar(tmp_path):
    """The gap the .shipped marker exists to close.

    Zero present bins is not a skip: the window still owns rows, so it must
    materialize and get marked, or it would be rediscovered forever.
    """
    lu, up, _ = make_uploader(tmp_path, ship_window_seconds=WINDOW)
    ws = int(NOW - 3 * WINDOW)

    add_metadata_row(tmp_path, FEED, ws + 10, digest=None, status_code=304)
    add_metadata_row(tmp_path, FEED, ws + 20, digest=None, status_code=304)

    ship_ready(lu)

    assert up.calls == []  # nothing to tar
    slice_path = window_metadata_local_path(tmp_path, FEED, ws)
    assert len(slice_path.read_text().splitlines()) == 2
    assert (tmp_path / FEED / ".shipped" / str(ws)).exists()
    assert lu._ready_raw_windows(NOW) == []  # and it stays handled


def test_raw_ships_before_metadata_is_materialized(tmp_path):
    """The documented invariant, on the success path.

    A reader who finds the shipped metadata slice must be able to find every
    digest it references already durable in S3 — so the PUT has to complete
    before the slice exists. Checked as its consequence: at the moment
    materialization starts, the tar is already uploaded.
    """
    lu, up, _ = make_uploader(tmp_path, ship_window_seconds=WINDOW)
    ws = int(NOW - 3 * WINDOW)
    add_response(tmp_path, FEED, ws + 10, payload=b"payload")

    calls: list[str] = []
    real_upload = up.upload
    real_materialize = lu._materialize_window_metadata

    def spy_upload(bucket, key, path):
        calls.append("upload")
        return real_upload(bucket, key, path)

    def spy_materialize(feed, window_start, rows):
        calls.append("materialize")
        return real_materialize(feed, window_start, rows)

    up.upload = spy_upload
    lu._materialize_window_metadata = spy_materialize

    ship_ready(lu)

    assert calls == ["upload", "materialize"]


def test_failed_upload_keeps_bins_and_materializes_nothing(tmp_path):
    """A failed PUT must leave the window fully retryable.

    No marker, no metadata slice, and the bins still on disk — so the next
    sweep rediscovers the window unchanged.
    """
    lu, up, _ = make_uploader(tmp_path, ship_window_seconds=WINDOW)
    ws = int(NOW - 3 * WINDOW)
    digest = add_response(tmp_path, FEED, ws + 10, payload=b"unshipped")

    attempts: list[str] = []

    def failing_upload(bucket, key, path):
        attempts.append(key)
        raise RuntimeError("S3 unavailable")

    up.upload = failing_upload

    ship_ready(lu)

    assert len(attempts) == 1  # it really did try
    bin_path = _day_dir(tmp_path, FEED, "raw", ws) / f"{digest}.bin"
    assert bin_path.read_bytes() == b"unshipped"
    assert not (tmp_path / FEED / ".shipped" / str(ws)).exists()
    assert not window_metadata_local_path(tmp_path, FEED, ws).exists()
    assert [w for _, w, _ in lu._ready_raw_windows(NOW)] == [ws]


def test_retry_after_materialize_failure_ships_under_a_distinct_key(
    tmp_path, monkeypatch
):
    """A second attempt must not clobber the first attempt's object.

    Models the one gap _ship_raw_window leaves open: the tar PUT succeeds and
    materialization then fails, so the bins are deliberately kept and the whole
    window is re-shipped next tick. Both objects are live in S3 at that point,
    so the shipped-at component of the key has to make them distinct.

    The clock is frozen because the key carries only whole seconds — two real
    calls inside one second would produce identical keys and the assertion
    would pass or fail on timing rather than on the key design.
    """
    lu, up, _ = make_uploader(tmp_path, ship_window_seconds=WINDOW)
    ws = int(NOW - 3 * WINDOW)
    digest = add_response(tmp_path, FEED, ws + 10, payload=b"retried")
    bin_path = _day_dir(tmp_path, FEED, "raw", ws) / f"{digest}.bin"

    clock = FrozenClock(NOW)
    monkeypatch.setattr("archiver.landing_uploader.time", clock)
    tars = capture_tars(up)

    state = {"materialize_fails": True}
    real_materialize = lu._materialize_window_metadata

    def flaky_materialize(feed, window_start, rows):
        if state["materialize_fails"]:
            raise OSError("no space left on device")
        return real_materialize(feed, window_start, rows)

    lu._materialize_window_metadata = flaky_materialize

    # attempt 1: tar lands in S3, then materialization dies
    ship_ready(lu)
    assert len(up.calls) == 1
    assert bin_path.exists()  # kept on purpose, so the window retries
    assert not (tmp_path / FEED / ".shipped" / str(ws)).exists()
    assert [w for _, w, _ in lu._ready_raw_windows(NOW)] == [ws]

    # attempt 2, one ship-window later, as _maybe_ship_raw_windows would
    state["materialize_fails"] = False
    clock.now = NOW + WINDOW
    ship_ready(lu, now=NOW + WINDOW)

    keys = [key for _, key, _ in up.calls]
    assert keys[0] != keys[1]  # the point: no clobber
    # lu._prefix, not a literal: this test is about the shipped-at component,
    # so it must not also encode whatever prefix make_uploader happens to set.
    assert keys == [
        window_tar_key(lu._prefix, FEED, ws, int(NOW)),
        window_tar_key(lu._prefix, FEED, ws, int(NOW + WINDOW)),
    ]

    # same window, same bytes -- a genuine re-ship, not a partial one
    assert tar_members(tars[0]) == tar_members(tars[1])

    assert not bin_path.exists()
    assert (tmp_path / FEED / ".shipped" / str(ws)).exists()
    assert lu._ready_raw_windows(NOW + WINDOW) == []
