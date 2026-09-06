"""Resolve a service date to a local GTFS zip via MobilityDatabase's real API.

Until 2026-09-06 this resolved snapshots via an unauthenticated third-party
mirror shaped like MBTA's `archived_feeds.txt`, whose only per-row identity
was a crawl timestamp. That mirror re-crawls a feed daily regardless of
whether the underlying schedule changed -- confirmed on GO_AHEAD/Entur
(mdb-1078): `feed_start_date` has been constant since 2019, but the mirror
stamped a brand-new timestamp on every single daily row. Since the old
`Snapshot.version_slug` derived from that timestamp, every day looked like a
new version, defeating pipeline/gtfs.py's per-version dedup (`written_versions`
/ `already_on_disk` in `process_feed_day`) for exactly the feeds whose
shapes.txt/stop_times.txt are big enough to OOM a container parsing them --
GO_AHEAD and TFNSW were rebuilding their full version marts from scratch every
night, forever, even on nights nothing actually changed.

MobilityDatabase's real API (api.mobilitydatabase.org) exposes a genuine
content hash per dataset (`hash`, SHA-256) -- confirmed live 2026-09-06 via
`GET /v1/gtfs_feeds/{feed_id}/datasets`, which returns a bare JSON list of
datasets, newest first, each with `hash`, `hosted_url`, `downloaded_at`,
`service_date_range_start`/`_end`. `Snapshot.version_slug` now derives from
that hash instead of the crawl timestamp: a re-crawl with unchanged content
resolves to the SAME version_slug as before, whose partition files already
exist on disk/S3, so pipeline/gtfs.py's existing skip logic -- unchanged,
nothing here needed to touch it -- never calls `for_date` at all on those
nights. `hosted_url` is fetchable without the bearer token (confirmed live),
so `ensure_local_zip` needs no auth.

Auth is OAuth2 refresh-token: `POST /v1/tokens` with `{"refresh_token": ...}`
returns `{access_token, expiration_datetime_utc, token_type}`; subsequent
calls send `Authorization: Bearer <access_token>`. The access token is cached
at module level (see `get_access_token`) since a `GtfsResolver` exists per
agency -- ~190+ instances in one `pipeline/gtfs.py` run -- and the token
itself is one MobilityDatabase-account-level credential, not per-agency.

The datasets endpoint pages (confirmed live: `limit=500` returned exactly
500 rows for GO_AHEAD's 2000+ days of daily crawl history) -- `fetch_catalog`
loops until a page comes back shorter than the requested page size.

Downloads are cached under `{cache_dir}/{agency}/v{version_slug}/feed.zip` so
repeated runs for the same snapshot don't re-fetch. A given Resolver instance
also memoizes loaded StaticGtfs objects so a date range that shares a snapshot
loads the zip exactly once.
"""

from __future__ import annotations

import datetime as dt
import time
from dataclasses import dataclass
from pathlib import Path

import pandas as pd
import requests

from analysis.static_gtfs import StaticGtfs

DEFAULT_API_URL = "https://api.mobilitydatabase.org/v1"
DEFAULT_CACHE_DIR = Path("data/static_gtfs")
_DATASETS_PAGE_SIZE = 500

# Cached access token, shared across every GtfsResolver in the process (one
# MobilityDatabase account, not per-agency). `_token_expires_at` is a
# time.monotonic() deadline computed from the token response's real
# expiration_datetime_utc, minus a safety margin -- not a guessed TTL, since
# the API tells us exactly when it expires.
_access_token: str | None = None
_token_expires_at: float | None = None
_TOKEN_REFRESH_MARGIN_S = 60


def _read_refresh_token() -> str:
    import os

    value = os.environ.get("MDB_REFRESH_TOKEN")
    if value is None:
        raise RuntimeError("Required env var 'MDB_REFRESH_TOKEN' is not set")
    return value


def _request_access_token(
    refresh_token: str, api_url: str = DEFAULT_API_URL
) -> tuple[str, float]:
    """POST the refresh token for a new access token. Returns (token, ttl_seconds)."""
    resp = requests.post(
        f"{api_url.rstrip('/')}/tokens",
        json={"refresh_token": refresh_token},
        timeout=30,
    )
    resp.raise_for_status()
    body = resp.json()
    expires_at = dt.datetime.fromisoformat(
        body["expiration_datetime_utc"].replace("Z", "+00:00")
    )
    ttl = (expires_at - dt.datetime.now(dt.timezone.utc)).total_seconds()
    return body["access_token"], ttl


def get_access_token(
    api_url: str = DEFAULT_API_URL, *, force_refresh: bool = False
) -> str:
    """A valid access token, fetching/refreshing as needed.

    Cached at module level with a deadline derived from the token's own
    real expiry (minus _TOKEN_REFRESH_MARGIN_S), not a guessed TTL -- a single
    stage-gtfs run processing ~190 agencies can plausibly outlive a 1hr token,
    so this has to actually refresh mid-run, not just fetch-once-and-forget.
    """
    global _access_token, _token_expires_at
    now = time.monotonic()
    if (
        force_refresh
        or _access_token is None
        or _token_expires_at is None
        or now >= _token_expires_at
    ):
        token, ttl = _request_access_token(_read_refresh_token(), api_url)
        _access_token = token
        _token_expires_at = now + max(ttl - _TOKEN_REFRESH_MARGIN_S, 0)
    return _access_token


def _reset_token_cache() -> None:
    """Test-only: clear the module-level token cache between tests."""
    global _access_token, _token_expires_at
    _access_token = None
    _token_expires_at = None


@dataclass(frozen=True)
class Snapshot:
    """One dataset from MobilityDatabase's `/gtfs_feeds/{id}/datasets`."""

    feed_start_date: dt.date
    feed_end_date: dt.date
    feed_version: (
        str  # downloaded_at (crawl timestamp) -- provenance/display only, NOT identity
    )
    archive_url: str  # sourced from hosted_url
    content_hash: str  # sourced from `hash` (SHA-256) -- this IS the identity

    @property
    def version_slug(self) -> str:
        """Filesystem-safe slug from content_hash -- stable across re-crawls
        of unchanged content, unlike the old feed_version-derived slug (see
        module docstring)."""
        return self.content_hash[:16]


def fetch_catalog(feed_id: str, api_url: str = DEFAULT_API_URL) -> pd.DataFrame:
    """GET every dataset MobilityDatabase has crawled for a feed_id, paginated.

    Returns a frame shaped like the pre-2026-09-06 CSV catalog (same column
    names) plus a new `content_hash` column, so pick_snapshot barely changes.
    Confirmed live 2026-09-06: the endpoint returns a bare JSON list (not a
    wrapped {"data": [...]} object), newest-crawl-first, and genuinely pages
    (a feed crawled daily since 2019 has 2000+ rows; `limit=500` returned
    exactly 500).
    """
    token = get_access_token(api_url)
    headers = {"Authorization": f"Bearer {token}"}
    url = f"{api_url.rstrip('/')}/gtfs_feeds/{feed_id}/datasets"

    rows: list[dict] = []
    offset = 0
    while True:
        resp = requests.get(
            url,
            headers=headers,
            params={"limit": _DATASETS_PAGE_SIZE, "offset": offset},
            timeout=30,
        )
        resp.raise_for_status()
        page = resp.json()
        rows.extend(page)
        if len(page) < _DATASETS_PAGE_SIZE:
            break
        offset += _DATASETS_PAGE_SIZE

    df = pd.DataFrame(
        {
            "feed_start_date": pd.to_datetime(
                [r.get("service_date_range_start") for r in rows],
                utc=True,
                errors="coerce",
            ),
            "feed_end_date": pd.to_datetime(
                [r.get("service_date_range_end") for r in rows],
                utc=True,
                errors="coerce",
            ),
            "feed_version": [r.get("downloaded_at") for r in rows],
            "archive_url": [r.get("hosted_url") for r in rows],
            "content_hash": [r.get("hash") for r in rows],
        }
    )
    # Drop rows missing critical fields. A dataset with no hash can't be given
    # a stable identity (see module docstring) -- treat it as unresolvable,
    # same as any other row missing a required field, rather than inventing
    # an identity from the URL/timestamp (which would silently reintroduce
    # the exact bug this module was rewritten to fix).
    df = df.dropna(
        subset=["feed_start_date", "feed_end_date", "archive_url", "content_hash"]
    )
    return df


def pick_snapshot(catalog: pd.DataFrame, target_date: dt.date) -> Snapshot:
    """The schedule in effect on `target_date`: latest-starting eligible row.

    Among catalog rows whose [feed_start_date, feed_end_date] span covers the
    date, the one with the latest feed_start_date is the schedule officially in
    effect. Ties break to the most recently crawled (feed_version, an ISO8601
    string that sorts chronologically as a plain string) -- explicit now,
    rather than incidentally depending on the API's row order the way the old
    CSV-order tie-break did (exactly the kind of implicit behavior that let
    the crawl-timestamp bug hide for as long as it did). Raises LookupError
    when no row covers the date.
    """
    target = pd.Timestamp(target_date, tz="UTC")
    eligible = catalog[
        (catalog["feed_start_date"] <= target) & (catalog["feed_end_date"] >= target)
    ]
    if eligible.empty:
        raise LookupError(f"No snapshot in catalog covers {target_date.isoformat()}")
    row = eligible.sort_values(
        ["feed_start_date", "feed_version"], ascending=[False, False]
    ).iloc[0]
    return Snapshot(
        feed_start_date=row["feed_start_date"].date(),
        feed_end_date=row["feed_end_date"].date(),
        feed_version=str(row["feed_version"]),
        archive_url=str(row["archive_url"]),
        content_hash=str(row["content_hash"]),
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
        self._loaded: dict[
            tuple[str, str | None, frozenset | None, frozenset | None], StaticGtfs
        ] = {}

    def catalog(self) -> pd.DataFrame:
        """The feed's archived-feeds catalog, fetched once and memoized."""
        if self._catalog is None:
            self._catalog = fetch_catalog(self.feed_id, self.api_url)
        return self._catalog

    def for_date(
        self,
        target_date: dt.date,
        agency_prefix: str | None = None,
        agency_ids: frozenset[str] | None = None,
        route_types: frozenset[int] | None = None,
    ) -> StaticGtfs:
        """StaticGtfs for the schedule in effect on `target_date`.

        Picks the snapshot from the catalog, downloads its zip if not cached,
        and loads it. Each distinct snapshot is downloaded and loaded once for
        the resolver's lifetime, so a date range sharing one schedule pays the
        cost a single time.

        `agency_prefix`/`agency_ids`/`route_types` are forwarded to StaticGtfs
        (see its docstring) for a zip covering multiple operators under one
        mdb_feed_id -- e.g. one agency_id's several feeds each scoped to a
        different operator within the same national feed (GO_AHEAD/Entur,
        TFNSW's/Transport for NSW's various modes). The cache key includes
        them, so the same snapshot requested under two different scopes gets
        two independently-filtered StaticGtfs instances rather than one shared
        (and, for that reason, unfiltered) one.
        """
        snap = pick_snapshot(self.catalog(), target_date)
        cache_key = (snap.version_slug, agency_prefix, agency_ids, route_types)
        if cache_key not in self._loaded:
            path = ensure_local_zip(snap, self.agency, self.cache_dir)
            self._loaded[cache_key] = StaticGtfs(
                path,
                agency_prefix=agency_prefix,
                agency_ids=agency_ids,
                route_types=route_types,
            )
        return self._loaded[cache_key]
