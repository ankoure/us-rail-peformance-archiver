"""Resolve a service date to a local GTFS zip via gtfs-archive-txt catalogs.

The catalog endpoint returns CSV in the MBTA `archived_feeds.txt` shape:

    feed_start_date,feed_end_date,feed_version,archive_url,archive_note
    20260520,20260907,2026-05-21T00:57:40Z,https://.../feed.zip,

Selection rule: among rows where `feed_start_date <= target_date <= feed_end_date`,
pick the one with the latest `feed_start_date` — that's the schedule officially
in effect on that day.

Downloads are cached under `{cache_dir}/{agency}/v{version_slug}/feed.zip` so
repeated runs for the same snapshot don't re-fetch. A given Resolver instance
also memoizes loaded StaticGtfs objects so a date range that shares a snapshot
loads the zip exactly once.
"""

from __future__ import annotations

import datetime as dt
import io
from dataclasses import dataclass
from pathlib import Path

import pandas as pd
import requests

from analysis.static_gtfs import StaticGtfs

DEFAULT_API_URL = "https://mwue7uiyf5.execute-api.us-east-1.amazonaws.com/api"
DEFAULT_CACHE_DIR = Path("data/static_gtfs")


@dataclass(frozen=True)
class Snapshot:
    """One row from the archived_feeds catalog."""

    feed_start_date: dt.date
    feed_end_date: dt.date
    feed_version: str
    archive_url: str

    @property
    def version_slug(self) -> str:
        """Filesystem-safe slug from feed_version, e.g. 20260521T005740."""
        # 2026-05-21T00:57:40.772601Z → 20260521T005740
        v = self.feed_version.split(".")[0].rstrip("Z")
        return v.replace("-", "").replace(":", "")


def fetch_catalog(feed_id: str, api_url: str = DEFAULT_API_URL) -> pd.DataFrame:
    """GET the archived_feeds.txt for a feed_id; returns a parsed frame."""
    url = f"{api_url.rstrip('/')}/archived_feeds.txt"
    resp = requests.get(url, params={"feed_id": feed_id}, timeout=30)
    resp.raise_for_status()
    df = pd.read_csv(
        io.BytesIO(resp.content),
        encoding="utf-8",
        dtype={"archive_url": str, "archive_note": str},
        parse_dates=["feed_start_date", "feed_end_date"],
        date_format="%Y%m%d",
    )
    # Drop rows missing critical fields.
    df = df.dropna(subset=["feed_start_date", "feed_end_date", "archive_url"])
    return df


def pick_snapshot(catalog: pd.DataFrame, target_date: dt.date) -> Snapshot:
    """The schedule in effect on `target_date`: latest-starting eligible row.

    Among catalog rows whose [feed_start_date, feed_end_date] span covers the
    date, the one with the latest feed_start_date is the schedule officially in
    effect. Raises LookupError when no row covers the date.
    """
    target = pd.Timestamp(target_date)
    eligible = catalog[
        (catalog["feed_start_date"] <= target) & (catalog["feed_end_date"] >= target)
    ]
    if eligible.empty:
        raise LookupError(f"No snapshot in catalog covers {target_date.isoformat()}")
    row = eligible.sort_values("feed_start_date", ascending=False).iloc[0]
    return Snapshot(
        feed_start_date=row["feed_start_date"].date(),
        feed_end_date=row["feed_end_date"].date(),
        feed_version=str(row["feed_version"]),
        archive_url=str(row["archive_url"]),
    )


def ensure_local_zip(
    snapshot: Snapshot,
    agency: str,
    cache_dir: Path | str = DEFAULT_CACHE_DIR,
) -> Path:
    """Download the snapshot zip if not already cached; return the local path."""
    dest = Path(cache_dir) / agency / f"v{snapshot.version_slug}" / "feed.zip"
    if dest.exists() and dest.stat().st_size > 0:
        return dest
    dest.parent.mkdir(parents=True, exist_ok=True)
    # Download to a tempfile and move on success so partial downloads don't poison the cache.
    tmp = dest.with_suffix(".zip.partial")
    with requests.get(snapshot.archive_url, stream=True, timeout=300) as resp:
        resp.raise_for_status()
        with tmp.open("wb") as f:
            for chunk in resp.iter_content(chunk_size=1024 * 1024):
                if chunk:
                    f.write(chunk)
    tmp.rename(dest)
    return dest


class GtfsResolver:
    """Catalog-aware resolver: date → StaticGtfs.

    One resolver per feed_id. Catalog is fetched once. Each unique snapshot is
    downloaded once. Each StaticGtfs is loaded once.
    """

    def __init__(
        self,
        feed_id: str,
        agency: str,
        cache_dir: Path | str = DEFAULT_CACHE_DIR,
        api_url: str = DEFAULT_API_URL,
    ) -> None:
        self.feed_id = feed_id
        self.agency = agency
        self.cache_dir = Path(cache_dir)
        self.api_url = api_url
        self._catalog: pd.DataFrame | None = None
        self._loaded: dict[tuple[str, str | None], StaticGtfs] = {}

    def catalog(self) -> pd.DataFrame:
        """The feed's archived-feeds catalog, fetched once and memoized."""
        if self._catalog is None:
            self._catalog = fetch_catalog(self.feed_id, self.api_url)
        return self._catalog

    def for_date(
        self, target_date: dt.date, agency_prefix: str | None = None
    ) -> StaticGtfs:
        """StaticGtfs for the schedule in effect on `target_date`.

        Picks the snapshot from the catalog, downloads its zip if not cached,
        and loads it. Each distinct snapshot is downloaded and loaded once for
        the resolver's lifetime, so a date range sharing one schedule pays the
        cost a single time.

        `agency_prefix` is forwarded to StaticGtfs (see its docstring) for a
        zip covering multiple operators under one mdb_feed_id -- e.g. one
        agency_id's several feeds each scoped to a different operator within
        the same national feed (GO_AHEAD/Entur). The cache key includes it, so
        the same snapshot requested under two different prefixes gets two
        independently-filtered StaticGtfs instances rather than one shared
        (and, for that reason, unfiltered) one.
        """
        snap = pick_snapshot(self.catalog(), target_date)
        cache_key = (snap.version_slug, agency_prefix)
        if cache_key not in self._loaded:
            path = ensure_local_zip(snap, self.agency, self.cache_dir)
            self._loaded[cache_key] = StaticGtfs(path, agency_prefix=agency_prefix)
        return self._loaded[cache_key]
