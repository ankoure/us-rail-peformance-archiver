"""Shared per-agency/per-feed S3 usage-and-cost scan.

Scans the cold and hot S3 buckets by feed prefix to compute actual storage
bytes/object counts, and estimates landing bucket usage from poll intervals
(too many small objects there to enumerate efficiently — see
scripts/s3_cost_report.py's original docstring for why).

Used by both scripts/s3_cost_report.py (manual CLI report) and
pipeline/s3_storage_metrics.py (the scheduled per-agency cost gauges).

Pricing is US-East-1, as of PRICING_DATE — a snapshot, not live AWS billing
data. AWS Cost Explorer can't break S3 cost down below the bucket level by
prefix/tag at all, so this kind of self-computed scan is the only way to get
a per-agency number; there's no path to a "real" one.
"""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from typing import Callable

from archiver.config import ArchiverConfig

# ── Pricing constants (US-East-1, as of PRICING_DATE) ──────────────────────
PRICING_DATE = "2026-06-20"
DEEP_ARCHIVE_GB_MONTH = 0.00099  # Glacier Deep Archive storage per GB/month
IT_GB_MONTH = 0.023  # Intelligent-Tiering (frequent-access tier) per GB/month
IT_MONITORING_PER_1K = 0.0025  # IT monitoring fee per 1,000 objects/month
STANDARD_GB_MONTH = 0.023  # Standard storage (landing bucket) per GB/month
PUT_COST_EACH = 0.000005  # $5.00 per million PUT requests
GET_COST_EACH = 0.0000004  # $0.40 per million GET requests
_GB = 1 << 30


@dataclass
class FeedCost:
    cold_bytes: int = 0
    cold_objects: int = 0
    hot_bytes: int = 0
    hot_objects: int = 0
    landing_est_bytes: float = 0.0
    put_count: float = 0.0
    get_count: float = 0.0

    @property
    def cold_usd(self) -> float:
        return (self.cold_bytes / _GB) * DEEP_ARCHIVE_GB_MONTH

    @property
    def hot_usd(self) -> float:
        storage = (self.hot_bytes / _GB) * IT_GB_MONTH
        monitoring = (self.hot_objects / 1_000) * IT_MONITORING_PER_1K
        return storage + monitoring

    @property
    def landing_usd(self) -> float:
        return (self.landing_est_bytes / _GB) * STANDARD_GB_MONTH

    @property
    def requests_usd(self) -> float:
        return self.put_count * PUT_COST_EACH + self.get_count * GET_COST_EACH

    @property
    def total_usd(self) -> float:
        return self.cold_usd + self.hot_usd + self.landing_usd + self.requests_usd

    def add(self, other: "FeedCost") -> None:
        self.cold_bytes += other.cold_bytes
        self.cold_objects += other.cold_objects
        self.hot_bytes += other.hot_bytes
        self.hot_objects += other.hot_objects
        self.landing_est_bytes += other.landing_est_bytes
        self.put_count += other.put_count
        self.get_count += other.get_count


# ── S3 helpers ───────────────────────────────────────────────────────────────


def _sum_bytes_under_prefix(client, bucket: str, prefix: str) -> tuple[int, int]:
    """Return (total_bytes, object_count) for all objects under prefix."""
    paginator = client.get_paginator("list_objects_v2")
    total_bytes = total_objects = 0
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            total_bytes += obj["Size"]
            total_objects += 1
    return total_bytes, total_objects


def hot_kind_prefixes(client, bucket: str, root_prefix: str) -> list[str]:
    """Discover per-kind prefixes in the hot bucket.

    The hot bucket uses kind-first partitioning:
      vehicles/feed=.../...
      metrics/route_day/feed=.../...

    We use list_objects_v2 with Delimiter='/' to enumerate kind 'directories'
    cheaply, then recurse one level into metrics/ to expand its sub-kinds.
    """
    resp = client.list_objects_v2(Bucket=bucket, Prefix=root_prefix, Delimiter="/")
    prefixes: list[str] = []
    metrics_prefix = f"{root_prefix}metrics/"
    for cp in resp.get("CommonPrefixes", []):
        p = cp["Prefix"]
        if p == metrics_prefix:
            sub = client.list_objects_v2(Bucket=bucket, Prefix=p, Delimiter="/")
            prefixes.extend(scp["Prefix"] for scp in sub.get("CommonPrefixes", []))
        else:
            prefixes.append(p)
    return prefixes


def _scan_cold(client, config: ArchiverConfig, feed_name: str) -> tuple[int, int]:
    prefix = f"{config.s3.cold_prefix}{feed_name}/"
    return _sum_bytes_under_prefix(client, config.s3.cold_bucket, prefix)


def _scan_hot(
    client, config: ArchiverConfig, feed_name: str, kind_prefixes: list[str]
) -> tuple[int, int]:
    total_bytes = total_objects = 0
    for kind_prefix in kind_prefixes:
        b, o = _sum_bytes_under_prefix(
            client, config.s3.hot_bucket, f"{kind_prefix}feed={feed_name}/"
        )
        total_bytes += b
        total_objects += o
    return total_bytes, total_objects


def estimate_landing(
    poll_interval_s: int,
    avg_msg_bytes: int,
    window_s: int,
) -> tuple[float, float, float]:
    """Estimate landing bucket usage over the 30-day lifecycle window.

    Returns (est_bytes_stored, put_count, get_count).

    Storage is the peak bytes in the bucket (30 days x daily writes). S3
    bills on time-weighted GB-months, so the real cost is ~half this; the
    estimate errs on the high side intentionally.

    One raw tar + one metadata .jsonl gets shipped per window (`window_s`
    seconds), so S3 sees 2 * (86400 / window_s) objects/feed/day.
    """
    polls_per_day = 86400 / poll_interval_s

    objects_per_day = 2 * (86400 / window_s)  # one .bin + one .jsonl per window
    est_bytes = polls_per_day * avg_msg_bytes * 30
    put_count = objects_per_day * 30  # 30 days of landing writes
    get_count = objects_per_day * 30  # rollup reads each object once
    return est_bytes, put_count, get_count


def scan_agencies(
    client,
    config: ArchiverConfig,
    *,
    kind_prefixes: list[str] | None = None,
    agencies: list[str] | None = None,
    feeds: list[str] | None = None,
    avg_msg_bytes: int = 50_000,
    workers: int = 8,
    progress: Callable[[int, int], None] | None = None,
) -> list[tuple[str, str, FeedCost]]:
    """Scan cold+hot for every matching feed and estimate landing.

    Returns (agency_name, feed_name, FeedCost) tuples sorted by (agency, feed).
    `agencies`/`feeds` filter by agency_id/feed name; None scans everything.
    `progress(done, total)` is called after each feed completes, if given.
    """
    if kind_prefixes is None:
        kind_prefixes = hot_kind_prefixes(
            client, config.s3.hot_bucket, config.s3.hot_prefix
        )

    work = [
        (agency, feed)
        for agency in config.agencies
        for feed in agency.feeds
        if (agencies is None or agency.agency_id in agencies)
        and (feeds is None or feed.name in feeds)
    ]

    def scan_one(agency_feed):
        agency, feed = agency_feed
        cold_b, cold_o = _scan_cold(client, config, feed.name)
        hot_b, hot_o = _scan_hot(client, config, feed.name, kind_prefixes)
        poll_interval = feed.poll_interval_seconds or 30
        est_bytes, put_count, get_count = estimate_landing(
            poll_interval,
            avg_msg_bytes,
            config.writer.ship_window_seconds,
        )
        return (
            agency.name,
            feed.name,
            FeedCost(
                cold_bytes=cold_b,
                cold_objects=cold_o,
                hot_bytes=hot_b,
                hot_objects=hot_o,
                landing_est_bytes=est_bytes,
                put_count=put_count,
                get_count=get_count,
            ),
        )

    results: list[tuple[str, str, FeedCost]] = []
    with ThreadPoolExecutor(max_workers=workers) as ex:
        futures = {ex.submit(scan_one, w): w for w in work}
        done = 0
        for future in as_completed(futures):
            results.append(future.result())
            done += 1
            if progress:
                progress(done, len(work))

    results.sort(key=lambda r: (r[0], r[1]))
    return results
