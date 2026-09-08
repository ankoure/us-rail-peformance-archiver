from __future__ import annotations

import asyncio
import json
import os
import tarfile
import tempfile
import threading
import time
from datetime import datetime, timezone
from pathlib import Path

from archiver.landing_layout import (
    iter_window_objects,
    window_metadata_local_path,
    window_object_key,
    window_tar_key,
)
from archiver.logger import logger
from archiver.telemetry import Telemetry
from archiver.uploader import Uploader

# How long __aexit__ waits for the worker to finish its in-flight upload before
# giving up. The thread is a daemon, so process exit reaps a stuck one; any
# object it didn't ship is still on disk and ships on the next boot's scan.
_JOIN_TIMEOUT_S = 60.0


# Prefix for the scratch directory _ship_raw_window stages a window's tar in
# before uploading. Lives under the landing dir (not TMPDIR) because that
# volume is provably sized for this data — it is already holding the raw
# files being tarred — whereas /tmp is tmpfs on most container and VM images,
# which would put the staged tar straight back into RAM. Dot-prefixed, so
# _feed_dirs (which treats every top-level landing_dir entry as a candidate
# feed name) skips it instead of treating a live scratch dir as a feed.
_SCRATCH_PREFIX = ".scratch-"

_WINDOW_GRACE_S = 60.0  # response.timestamp is stamped at construction, not
# write-completion -- a poll straddling the boundary
# needs a moment to land before its window is "closed"


class LandingUploader:
    """Drains local landing-zone objects to S3 on a background thread.

    Lifecycle is RAII via the async context-manager protocol so it nests in the
    poller's AsyncExitStack alongside the agency clients (see main.run). It owns
    exactly one worker thread for its whole lifetime, running two independent
    cadences from the same loop (see ``_run``):

    Metadata ships one small ``window=*.jsonl`` object at a time, as soon as
    one is materialized (``_scan_once_window``, paced by ``scan_interval``) --
    this half is unchanged from before content-addressed raw storage existed.

    Raw payloads ship as one uncompressed tar per window (``_maybe_ship_raw_windows``,
    paced independently by ``ship_window_seconds``, since PUT count against S3
    is exactly what that cadence exists to bound). ``ContentAddressedWriter``
    writes each poll's payload immediately to a content-hash-named
    ``{digest}.bin`` -- there's no ``window=*.bin`` file for a glob to find the
    way there is for metadata, so a closed window's digests are discovered by
    reading the feed's local ``data.jsonl`` directly (``_closed_windows``).
    Whichever of those digests still has a ``{digest}.bin`` present locally
    gets tarred and uploaded (``_ship_raw_window``); a ``.shipped`` marker then
    records that the window's metadata was handed off, because bin-absence
    alone can't mean "already shipped" here -- see ``_shipped_marker``'s
    docstring for why.
    """

    def __init__(
        self,
        landing_dir: str | Path,
        uploader: Uploader,
        bucket: str,
        prefix: str = "",
        *,
        telemetry: Telemetry,
        scan_interval: float = 30.0,
        ship_window_seconds: float = 3600.0,
        feed_names: set[str] | None = None,
    ) -> None:
        if prefix and not prefix.endswith("/"):
            # Keys must stay byte-identical to the old synchronous S3Sink path;
            # a slash-less prefix would silently fuse onto the first key
            # segment ("data" + "raw/x.bin" -> "dataraw/x.bin"). Fail loudly at
            # construction instead of producing a misplaced object at runtime.
            raise ValueError(f"prefix must end with '/' (got {prefix!r})")

        self._landing_dir = Path(landing_dir)
        self._uploader = uploader
        self._bucket = bucket
        self._prefix = prefix
        self._tel = telemetry
        self._scan_interval = scan_interval
        self._feed_names = feed_names
        self._ship_window_seconds = ship_window_seconds
        self._next_raw_ship_due = 0.0

        # Set on shutdown. The worker waits on it WITH A TIMEOUT, so the same
        # primitive both paces scans and wakes the thread promptly to stop.
        self._stop = threading.Event()
        # armed until the first match; runs on each empty scan until then bounded,
        # because next(...) short-circuits at the first stray and it only runs while pending is empty.
        self._layout_checked = False
        # Created in __aenter__: threads are single-use, and creating it there
        # keeps __aexit__ safe to call even if entry never happened.
        self._thread: threading.Thread | None = None

    # --- lifecycle: async CM so it nests in the poller's AsyncExitStack ------

    async def __aenter__(self) -> "LandingUploader":
        self._thread = threading.Thread(
            target=self._run, name="landing-uploader", daemon=True
        )
        # The worker scans immediately before its first wait, so this start IS
        # the boot-recovery scan for anything left over from a previous run.
        self._thread.start()
        return self

    async def __aexit__(self, _exc_type, _exc, _tb) -> None:
        self._stop.set()
        if self._thread is not None:
            # join() blocks, so offload it off the event loop. Bounded: a
            # worker stuck deep in boto3 retries must not hang shutdown it's
            # a daemon thread, and whatever it didn't ship is still on disk.
            await asyncio.to_thread(self._thread.join, _JOIN_TIMEOUT_S)
            if self._thread.is_alive():
                logger.warning(
                    "landing-uploader thread did not stop within %.0fs; "
                    "abandoning it (daemon). Pending objects ship next boot.",
                    _JOIN_TIMEOUT_S,
                )
                return  # don't run the drain concurrently with the worker

        # Final best-effort drain: the stack unwinds AFTER the loop's finally
        # (flush_all), so the last windows are already on disk  ship them now
        # so a clean shutdown leaves nothing behind. ignore_stop because _stop
        # is set; the pass is still bounded (one sweep of current files). Any
        # failure here is fine: the remainder ships on the next boot's scan.
        try:
            await asyncio.to_thread(self._drain_once)
        except Exception:
            logger.exception("final landing drain failed; remainder ships on next boot")

    def _drain_once(self) -> None:
        """One bounded shutdown sweep: raw tars first, then the file scan.

        Raw goes first so any window it materializes is visible to the metadata
        sweep that follows, and the whole thing lands in a single pass instead
        of leaving freshly-written metadata for the next boot.
        """
        try:
            self._maybe_ship_raw_windows(ignore_stop=True, ignore_cadence=True)
        except Exception:
            logger.exception("final raw drain failed; remainder ships on next boot")
        self._scan_once_window(ignore_stop=True)

    # --- worker -------------------------------------------------------------

    def _run(self) -> None:
        """Worker-thread entrypoint: scan -> ship -> wait, until stopped."""
        while not self._stop.is_set():
            # Two independent try blocks, not one around both: the metadata
            # sweep and the raw-tar cadence are unrelated, and a failure in
            # either must cost only its own tick. One bad scan must never kill
            # the thread, and must never skip the other cadence's work.
            try:
                self._scan_once_window()
            except Exception:
                logger.exception("landing scan failed; retrying next interval")
            try:
                self._maybe_ship_raw_windows()
            except Exception:
                logger.exception("raw window ship failed; retrying next interval")
            if self._stop.wait(timeout=self._scan_interval):
                break

    # --- raw window tars ----------------------------------------------------

    def _maybe_ship_raw_windows(
        self, ignore_stop: bool = False, ignore_cadence: bool = False
    ) -> None:
        """Ship closed raw windows as tars, at most once per ship window.

        Paced independently of the metadata sweep: raw objects are large and
        their PUT cost is what the cadence exists to bound. The next due time
        is advanced BEFORE any work, so a failing upload waits a full window
        rather than retrying every scan interval — the windows stay on disk
        either way, so the cost of the delay is latency, not data.
        """
        now = time.time()
        if not ignore_cadence:
            if now < self._next_raw_ship_due:
                return
            self._next_raw_ship_due = now + self._ship_window_seconds

        ready = self._ready_raw_windows(now)
        self._tel.gauge("landing.raw_pending", len(ready))
        for feed, window_start, rows in ready:
            if self._stop.is_set() and not ignore_stop:
                return
            self._ship_raw_window(feed, window_start, rows)

    def _feed_dirs(self) -> set[str]:
        """Feeds to check for raw windows: explicit list if given, else every
        top-level directory under landing_dir (there's no glob to lean on for
        raw discovery anymore, so this replaces what iter_window_objects's
        '*/...' pattern used to cover implicitly).

        Dot-prefixed directories are skipped: _SCRATCH_PREFIX tempdirs live
        directly under landing_dir, and a live tar-staging scratch dir would
        otherwise be picked up as a feed on the very next tick.
        """
        if self._feed_names is not None:
            return self._feed_names
        try:
            return {
                p.name
                for p in self._landing_dir.iterdir()
                if p.is_dir() and not p.name.startswith(".")
            }
        except OSError:
            return set()

    @staticmethod
    def _day_partition(unix_ts: float) -> tuple[str, str, str]:
        """('year=YYYY', 'month=M', 'day=D') for a UTC timestamp.

        Month and day are UNPADDED on purpose: BaseWriter.append_metadata and
        ContentAddressedWriter._write_sync both format with a bare
        f"month={date.month}", so "month=6/day=3" is what is actually on disk.
        Zero-padding here would silently miss every partition before the 10th
        of a month — the sweep would find nothing and log nothing.
        """
        d = datetime.fromtimestamp(unix_ts, tz=timezone.utc)
        return f"year={d.year}", f"month={d.month}", f"day={d.day}"

    def _shipped_marker(self, feed: str, window_start: int) -> Path:
        """Sentinel proving this window's metadata slice was already handed off.

        Needed because "all this window's bins are gone" CANNOT mean "already
        shipped" under ContentAddressedWriter: identical content across
        consecutive windows resolves to ONE {digest}.bin, so a window whose
        content never changed legitimately has zero bins of its own. Using
        bin-absence as the marker would drop those windows' metadata entirely.

        And the materialized window=*.jsonl can't serve as the marker either --
        _scan_once_window deletes it on successful upload, after which the rows
        are still in the append-only data.jsonl and the window would look
        unhandled again on every subsequent sweep, forever.

        Named without a "window=" prefix so it matches neither the window-object
        globs nor _warn_if_layout_mismatch's rglob.
        """
        return self._landing_dir / feed / ".shipped" / str(window_start)

    def _prune_shipped_markers(self, feed: str, now: float) -> None:
        """Drop markers for windows older than the metadata we still read.

        _closed_windows only ever looks at today's and yesterday's data.jsonl,
        so a window more than two days back can never be rediscovered and its
        marker is dead weight. Without this the directory grows by one entry
        per window forever (288/day at a 300 s cadence).
        """
        cutoff = now - 2 * 86400.0
        marker_dir = self._landing_dir / feed / ".shipped"
        try:
            for entry in marker_dir.iterdir():
                try:
                    if int(entry.name) < cutoff:
                        entry.unlink(missing_ok=True)
                except ValueError:
                    continue
        except OSError:
            return

    def _metadata_jsonl(self, feed: str, unix_ts: float) -> Path:
        year, month, day = self._day_partition(unix_ts)
        return self._landing_dir / feed / "metadata" / year / month / day / "data.jsonl"

    def _raw_dir(self, feed: str, unix_ts: float) -> Path:
        year, month, day = self._day_partition(unix_ts)
        return self._landing_dir / feed / "raw" / year / month / day

    def _closed_windows(self, feed: str, now: float) -> dict[int, list[dict]]:
        """{window_start: [row, ...]} for every CLOSED window found in this
        feed's local metadata, grouped by window index * ship_window_seconds.

        Reads today's AND yesterday's data.jsonl. Yesterday matters for a
        bounded stretch after 00:00 UTC: the last window of the previous day
        only becomes closed once the grace period elapses, by which point the
        writer has already rolled to a new day-partition file. Reading only
        today would strand it until the process restarted. (Windows themselves
        never straddle midnight as long as ship_window_seconds divides a day,
        which both 300 and 3600 do — so a window's rows live in exactly one
        file.)

        Malformed lines are logged and skipped rather than killing the sweep,
        matching payloads.digest_timestamps' handling of the same file.
        """
        buckets: dict[int, list[dict]] = {}
        window = self._ship_window_seconds
        for ts in (now, now - 86400.0):
            path = self._metadata_jsonl(feed, ts)
            try:
                with path.open("r", encoding="utf-8") as fh:
                    for lineno, line in enumerate(fh, 1):
                        line = line.strip()
                        if not line:
                            continue
                        try:
                            row = json.loads(line)
                            row_ts = float(row["timestamp"])
                        except (ValueError, TypeError, KeyError):
                            logger.warning(
                                "skipping malformed metadata line %s:%d", path, lineno
                            )
                            continue
                        window_start = int(row_ts // window) * int(window)
                        buckets.setdefault(window_start, []).append(row)
            except FileNotFoundError:
                continue
            except OSError:
                logger.exception("could not read landing metadata: %s", path)
                continue

        # now >= window_start + window + grace, rearranged so the arithmetic
        # happens once rather than per bucket.
        cutoff = now - window - _WINDOW_GRACE_S
        return {ws: rows for ws, rows in buckets.items() if ws <= cutoff}

    def _ready_raw_windows(self, now: float) -> list[tuple[str, int, list[dict]]]:
        """(feed, window_start, rows) for every closed window not yet handed off.

        Readiness is metadata-driven, not bin-driven: a closed window is ready
        if it has rows and no shipped-marker. Whether it also has raw bytes to
        tar is _ship_raw_window's problem -- an all-304 window, or one whose
        content was unchanged from the previous window (same digest, same
        already-shipped file), still owns metadata rows that have to reach S3.

        Re-derived from live file state every call, not a marching cursor --
        see the note above _WINDOW_GRACE_S / on _maybe_ship_raw_windows.
        """
        ready = []
        for feed in self._feed_dirs():
            self._prune_shipped_markers(feed, now)
            for window_start, rows in self._closed_windows(feed, now).items():
                if not rows:
                    continue
                if self._shipped_marker(feed, window_start).exists():
                    continue
                ready.append((feed, window_start, rows))
        ready.sort(key=lambda item: (item[1], item[0]))  # oldest window first
        return ready

    def _ship_raw_window(self, feed: str, window_start: int, rows: list[dict]) -> None:
        """Tar every still-present {digest}.bin for this window, ship it, then
        materialize + hand off the window's metadata slice.

        Ordering matters: raw ships BEFORE metadata is materialized (mirrors
        BatchingWriter._write_buckets' old "bin first, then metadata" -- a
        reader who sees the shipped metadata object should always be able to
        find its referenced digests already durable in S3). Delete local
        {digest}.bin files ONLY after a confirmed upload -- same crash-safety
        invariant _merge_and_ship had.

        Deletion is deferred one step further than the old writer did it: the
        bins go only after metadata is materialized, not immediately after the
        PUT. A crash in that gap would otherwise leave a window whose digests
        are all absent, which _ready_raw_windows reads as "already shipped" --
        so nothing would ever materialize it and the metadata would be lost.
        Keeping the bins until metadata exists makes the window re-enter the
        ready set on the next tick instead.
        """
        raw_dir = self._raw_dir(feed, window_start)
        present: list[Path] = []
        seen: set[str] = set()
        for row in rows:
            digest = row.get("digest")
            if not digest or digest in seen:
                continue  # 304s carry no digest; dups share one bin
            seen.add(digest)
            path = raw_dir / f"{digest}.bin"
            if path.exists():
                present.append(path)

        # No bins is NOT an error and NOT a skip: an all-304 window, or one
        # whose payload was byte-identical to an earlier window's (content
        # addressing collapses both to one already-shipped file), has metadata
        # rows and nothing to tar. Fall straight through to materialization --
        # every digest those rows reference is already durable in S3, so the
        # raw-before-metadata invariant still holds.
        if present:
            key = window_tar_key(self._prefix, feed, window_start, int(time.time()))
            try:
                with tempfile.TemporaryDirectory(
                    dir=self._landing_dir, prefix=_SCRATCH_PREFIX
                ) as scratch:
                    staged = Path(scratch) / f"window={window_start}.tar"
                    with tarfile.open(staged, "w") as tar:
                        for path in present:
                            try:
                                tar.add(path, arcname=path.name)
                            except FileNotFoundError:
                                # Raced with something else; the metadata row
                                # still points at it, so let the next tick decide.
                                logger.warning("raw file vanished mid-tar: %s", path)
                    self._tel.gauge("landing.raw_tar_bytes", staged.stat().st_size)
                    self._uploader.upload(self._bucket, key, staged)
            except Exception:
                logger.exception(
                    "raw window upload failed: feed=%s window=%d; will retry next window",
                    feed,
                    window_start,
                )
                return

        try:
            self._materialize_window_metadata(feed, window_start, rows)
        except Exception:
            logger.exception(
                "raw tar shipped but metadata materialize failed: feed=%s window=%d; "
                "keeping local bins so the window retries",
                feed,
                window_start,
            )
            return

        # Marker before unlink, and only once the slice is durably on disk. A
        # crash in the gap re-materializes the same file next tick (same bytes,
        # same path -- idempotent); a crash before the marker at worst costs one
        # duplicate metadata object, which is a deterministic key and overwrites.
        marker = self._shipped_marker(feed, window_start)
        try:
            marker.parent.mkdir(parents=True, exist_ok=True)
            marker.touch()
        except OSError:
            logger.exception("could not write shipped marker: %s", marker)
            return  # keep the bins; re-shipping is cheaper than losing them

        for path in present:
            path.unlink(missing_ok=True)

    def _materialize_window_metadata(
        self, feed: str, window_start: int, rows: list[dict]
    ) -> None:
        """Write this window's metadata slice where _scan_once_window will find
        it, via tmp-write + rename (mirrors write_bytes_atomic's pattern).

        The scratch name keeps the ``.tmp`` suffix that both the window globs
        and _warn_if_layout_mismatch filter on, so a half-written slice can
        never be picked up as shippable.
        """
        dest = window_metadata_local_path(self._landing_dir, feed, window_start)
        dest.parent.mkdir(parents=True, exist_ok=True)
        payload = "".join(
            json.dumps(row, separators=(",", ":"), sort_keys=True) + "\n"
            for row in rows
        ).encode("utf-8")

        tmp = dest.with_name(dest.name + ".tmp")
        try:
            with tmp.open("wb") as fh:
                fh.write(payload)
                fh.flush()
                os.fsync(fh.fileno())
            tmp.replace(dest)
        except Exception:
            tmp.unlink(missing_ok=True)
            raise

    # --- window (per-file) scan — original behaviour -----------------------

    def _scan_once_window(self, ignore_stop: bool = False) -> None:
        """Ship every pending window object currently on disk, oldest first."""
        pending = self._pending()
        self._tel.gauge("landing.pending", len(pending))
        for path in pending:
            if self._stop.is_set() and not ignore_stop:
                return
            self._ship_one(path)

    def _pending(self) -> list[Path]:
        """Window objects awaiting shipment, oldest first by mtime."""
        candidates = [
            p
            for p in iter_window_objects(self._landing_dir)
            if self._feed_names is None
            or p.relative_to(self._landing_dir).parts[0] in self._feed_names
        ]

        if not self._layout_checked:
            if candidates:
                self._layout_checked = True
            else:
                self._warn_if_layout_mismatch()

        def mtime(p: Path) -> float:
            try:
                return p.stat().st_mtime
            except OSError:
                return float("inf")

        return sorted(candidates, key=mtime)

    def _warn_if_layout_mismatch(self) -> None:
        """Detect stale globs: window-like files exist but no pattern matches."""
        try:
            stray = next(
                (
                    p
                    for p in self._landing_dir.rglob("window=*")
                    if p.is_file() and p.suffix != ".tmp"
                ),
                None,
            )
        except OSError:
            return
        if stray is not None:
            logger.error(
                "landing layout mismatch: %s exists but matches no pattern in "
                "landing_layout.WINDOW_OBJECT_GLOBS - globs are likely stale vs "
                "writer.py; NOTHING will ship until they are fixed",
                stray,
            )

    def _ship_one(self, path: Path) -> None:
        """Upload one object; delete on success."""
        key = self._key_for(path)
        try:
            self._uploader.upload(self._bucket, key, path)
        except FileNotFoundError:
            return
        except Exception:
            logger.exception(
                "landing upload failed, will retry next scan: %s -> s3://%s/%s",
                path,
                self._bucket,
                key,
            )
            return
        path.unlink(missing_ok=True)

    def _key_for(self, path: Path) -> str:
        return window_object_key(self._landing_dir, path, self._prefix)
