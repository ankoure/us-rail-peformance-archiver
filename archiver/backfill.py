"""One-shot landing backfill: ship local window objects to S3, exists()-gated.

Soak-phase parity tool from the original local -> s3 landing_mode migration,
the batch counterpart to the continuous LandingUploader. Differences, all
because the contexts differ:
  - exists()-gated, not delete-on-ship: idempotent and re-runnable; ships only
    what S3 is missing. HEADs are cheap at batch cadence.
  - parallel (ThreadPoolExecutor), not single-threaded: this is a catch-up batch
    where throughput matters, unlike the poller's low steady rate.
  - never deletes: during the soak local is authoritative; this only populates
    and verifies S3.

METADATA-ONLY as of the move to ContentAddressedWriter: selection comes from
archiver.landing_layout.iter_window_objects, whose WINDOW_OBJECT_GLOBS now
matches only metadata window=*.jsonl files -- raw payloads are content-hash-named
(no window=*.bin to glob) and shipped as per-window tars by LandingUploader's
own _ship_raw_window, which this tool has no counterpart for. That's a
deliberate scope decision, not an oversight: this was a one-time migration
tool for the original soak, not something run in ongoing operation, so it
wasn't worth extending for a storage format that arrived well after that soak
ended. If it ever needs to run again against data that includes raw
content-addressed payloads, it will only backfill/verify metadata -- raw
gaps will not be caught by run() or verify() and would need a separate pass.

Key mapping (window_object_key) is shared verbatim with LandingUploader so the
keys are identical -- otherwise the parity check would be the source of the
discrepancy.
"""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

from archiver.landing_layout import iter_window_objects, window_object_key
from archiver.logger import logger
from archiver.telemetry import NoOpTelemetry, Telemetry
from archiver.uploader import Uploader


class LandingBackfill:
    def __init__(
        self,
        landing_dir: str | Path,
        uploader: Uploader,
        bucket: str,
        prefix: str = "",
        *,
        telemetry: Telemetry | None = None,
    ) -> None:
        if prefix and not prefix.endswith("/"):
            # Same invariant as LandingUploader: keys must stay byte-identical to
            # the S3Sink layout, so a slash-less prefix is a misconfig, not a
            # silent key fusion.
            raise ValueError(f"prefix must end with '/' (got {prefix!r})")
        self._landing_dir = Path(landing_dir)
        self._uploader = uploader
        self._bucket = bucket
        self._prefix = prefix
        self._tel = telemetry or NoOpTelemetry()

    def run(self, *, force: bool = False, workers: int = 8) -> dict[str, int]:
        """Upload every local window object missing from S3 (or all, if force).

        Idempotent: re-running ships only the gap. Returns counts so the caller
        (and the soak operator) can see total / uploaded / already-present /
        failed at a glance.
        """
        objs = sorted(iter_window_objects(self._landing_dir))
        counts = {"total": len(objs), "uploaded": 0, "skipped": 0, "failed": 0}
        if not objs:
            logger.warning(
                "backfill: no window objects under %s -- wrong dir, or globs "
                "stale vs writer.py?",
                self._landing_dir,
            )
            return counts

        with ThreadPoolExecutor(max_workers=workers) as ex:
            futures = {ex.submit(self._ship_one, p, force): p for p in objs}
            for fut in as_completed(futures):
                path = futures[fut]
                try:
                    counts[fut.result()] += 1
                except Exception:
                    logger.exception("backfill failed: %s", path)
                    counts["failed"] += 1

        logger.info(
            "backfill: %d total, %d uploaded, %d already in s3, %d failed",
            counts["total"],
            counts["uploaded"],
            counts["skipped"],
            counts["failed"],
        )
        return counts

    def _ship_one(self, path: Path, force: bool) -> str:
        key = window_object_key(self._landing_dir, path, self._prefix)
        if not force and self._uploader.exists(self._bucket, key):
            self._tel.incr("backfill.skipped")
            return "skipped"
        self._uploader.upload(self._bucket, key, path)  # emits s3.request{op:put}
        self._tel.incr("backfill.uploaded")
        return "uploaded"

    def verify(self, *, sample: int = 20) -> dict[str, int]:
        """Report local-vs-S3 window-key parity without uploading.

        `missing_in_s3` (local keys absent from S3) is the actionable gap a
        `run()` would close. `only_in_s3` is expected and benign -- S3 retains
        windows that local has already pruned (3-day buffer), so it's reported
        for visibility, not as an error.
        """
        local = {
            window_object_key(self._landing_dir, p, self._prefix)
            for p in iter_window_objects(self._landing_dir)
        }
        s3 = set(self._uploader.list_keys(self._bucket, self._prefix))
        missing_in_s3 = local - s3
        only_in_s3 = s3 - local

        logger.info(
            "parity: %d local, %d s3, %d missing_in_s3, %d only_in_s3 (pruned-local, benign)",
            len(local),
            len(s3),
            len(missing_in_s3),
            len(only_in_s3),
        )
        for key in sorted(missing_in_s3)[:sample]:
            logger.info("  missing_in_s3: %s", key)

        return {
            "local": len(local),
            "s3": len(s3),
            "missing_in_s3": len(missing_in_s3),
            "only_in_s3": len(only_in_s3),
        }
