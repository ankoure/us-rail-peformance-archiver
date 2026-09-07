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

import hashlib
import io
import struct

from archiver import landing_uploader as mod
from archiver.landing_uploader import LandingUploader
from archiver.writer import HEADER, FrameReader

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
