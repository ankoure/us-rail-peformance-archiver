"""Tests for analysis/gtfs_fetcher.py — HTTP is monkeypatched, no network."""

from __future__ import annotations

import datetime as dt
import io
import zipfile
from dataclasses import dataclass, field

import pandas as pd
import pytest

import analysis.gtfs_fetcher as gtfs_fetcher
from analysis.gtfs_fetcher import (
    GtfsResolver,
    Snapshot,
    ensure_local_zip,
    fetch_catalog,
    get_access_token,
    pick_snapshot,
)

# Three datasets for one feed, newest crawl first — matches the real API's
# observed order (see module docstring). "hash-v3" == "hash-v3" further down
# lets a test simulate a re-crawl of unchanged content.
SAMPLE_DATASETS = [
    {
        "id": "mdb-1847-20260521",
        "feed_id": "mdb-1847",
        "hosted_url": "https://example/v3.zip",
        "downloaded_at": "2026-05-21T00:57:40.772601Z",
        "hash": "hash-v3",
        "hash_md5": "md5-v3",
        "service_date_range_start": "2026-05-20T00:00:00Z",
        "service_date_range_end": "2026-09-07T00:00:00Z",
    },
    {
        "id": "mdb-1847-20260520",
        "feed_id": "mdb-1847",
        "hosted_url": "https://example/v2.zip",
        "downloaded_at": "2026-05-20T01:19:20.442659Z",
        "hash": "hash-v2",
        "hash_md5": "md5-v2",
        "service_date_range_start": "2026-05-19T00:00:00Z",
        "service_date_range_end": "2026-09-07T00:00:00Z",
    },
    {
        "id": "mdb-1847-20260515",
        "feed_id": "mdb-1847",
        "hosted_url": "https://example/v1.zip",
        "downloaded_at": "2026-05-15T01:15:18.486701Z",
        "hash": "hash-v1",
        "hash_md5": "md5-v1",
        "service_date_range_start": "2026-05-14T00:00:00Z",
        "service_date_range_end": "2026-09-07T00:00:00Z",
    },
]

TOKEN_RESPONSE = {
    "access_token": "fake-access-token",
    "expiration_datetime_utc": "2099-01-01T00:00:00Z",  # far future — TTL tests override via monkeypatched time
    "token_type": "Bearer",
}


@dataclass
class FakeResponse:
    status_code: int = 200
    text: str = ""
    content: bytes = b""
    _json: object = field(default=None)

    def __post_init__(self) -> None:
        if self.text and not self.content:
            self.content = self.text.encode("utf-8")

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise RuntimeError(f"HTTP {self.status_code}")

    def json(self):
        return self._json

    def iter_content(self, chunk_size: int):
        for i in range(0, len(self.content), chunk_size):
            yield self.content[i : i + chunk_size]

    def __enter__(self):
        return self

    def __exit__(self, *args):
        pass


@pytest.fixture(autouse=True)
def _reset_token_cache():
    """The access-token cache is module-level global state (shared across
    every GtfsResolver in a real process) -- reset it before each test so
    tests don't leak a cached token into each other."""
    gtfs_fetcher._reset_token_cache()
    yield
    gtfs_fetcher._reset_token_cache()


def _fake_post_token(**overrides):
    def fake_post(url, json=None, timeout=None):
        assert url.endswith("/tokens")
        assert json == {"refresh_token": "the-refresh-token"}
        return FakeResponse(_json={**TOKEN_RESPONSE, **overrides})

    return fake_post


class TestGetAccessToken:
    def test_fetches_and_caches(self, monkeypatch):
        monkeypatch.setenv("MDB_REFRESH_TOKEN", "the-refresh-token")
        calls = {"n": 0}

        def fake_post(url, json=None, timeout=None):
            calls["n"] += 1
            return FakeResponse(_json=TOKEN_RESPONSE)

        monkeypatch.setattr("analysis.gtfs_fetcher.requests.post", fake_post)
        token = get_access_token(api_url="https://example/api")
        assert token == "fake-access-token"
        assert calls["n"] == 1

    def test_reused_across_calls_without_refetching(self, monkeypatch):
        monkeypatch.setenv("MDB_REFRESH_TOKEN", "the-refresh-token")
        calls = {"n": 0}

        def fake_post(url, json=None, timeout=None):
            calls["n"] += 1
            return FakeResponse(_json=TOKEN_RESPONSE)

        monkeypatch.setattr("analysis.gtfs_fetcher.requests.post", fake_post)
        get_access_token()
        get_access_token()
        get_access_token()
        assert calls["n"] == 1

    def test_refreshes_after_ttl_elapses(self, monkeypatch):
        monkeypatch.setenv("MDB_REFRESH_TOKEN", "the-refresh-token")
        # Expire almost immediately so the margin pushes the deadline into the past.
        near_now = (
            dt.datetime.now(dt.timezone.utc) + dt.timedelta(seconds=30)
        ).strftime("%Y-%m-%dT%H:%M:%SZ")
        calls = {"n": 0}

        def fake_post(url, json=None, timeout=None):
            calls["n"] += 1
            return FakeResponse(
                _json={**TOKEN_RESPONSE, "expiration_datetime_utc": near_now}
            )

        monkeypatch.setattr("analysis.gtfs_fetcher.requests.post", fake_post)
        get_access_token()
        assert calls["n"] == 1
        # TTL (30s) minus the 60s safety margin is negative, so the cached
        # deadline is already in the past -- the very next call must refetch.
        get_access_token()
        assert calls["n"] == 2

    def test_missing_refresh_token_env_var_raises(self, monkeypatch):
        monkeypatch.delenv("MDB_REFRESH_TOKEN", raising=False)
        with pytest.raises(RuntimeError, match="MDB_REFRESH_TOKEN"):
            get_access_token()


class TestFetchCatalog:
    def test_parses_json_response_and_sends_bearer_token(self, monkeypatch):
        monkeypatch.setenv("MDB_REFRESH_TOKEN", "the-refresh-token")
        monkeypatch.setattr("analysis.gtfs_fetcher.requests.post", _fake_post_token())
        captured = {}

        def fake_get(url, headers=None, params=None, timeout=None):
            captured["url"] = url
            captured["headers"] = headers
            captured["params"] = params
            return FakeResponse(_json=SAMPLE_DATASETS)

        monkeypatch.setattr("analysis.gtfs_fetcher.requests.get", fake_get)
        df = fetch_catalog("mdb-1847", api_url="https://example/api")

        assert captured["url"] == "https://example/api/gtfs_feeds/mdb-1847/datasets"
        assert captured["headers"] == {"Authorization": "Bearer fake-access-token"}
        assert len(df) == 3
        assert set(df.columns) >= {
            "feed_start_date",
            "feed_end_date",
            "feed_version",
            "archive_url",
            "content_hash",
        }

    def test_paginates_until_a_short_page(self, monkeypatch):
        monkeypatch.setenv("MDB_REFRESH_TOKEN", "the-refresh-token")
        monkeypatch.setattr("analysis.gtfs_fetcher.requests.post", _fake_post_token())
        # Page size 2 for this test via monkeypatching the module constant.
        monkeypatch.setattr("analysis.gtfs_fetcher._DATASETS_PAGE_SIZE", 2)
        offsets_seen = []

        def fake_get(url, headers=None, params=None, timeout=None):
            offsets_seen.append(params["offset"])
            page = SAMPLE_DATASETS[params["offset"] : params["offset"] + 2]
            return FakeResponse(_json=page)

        monkeypatch.setattr("analysis.gtfs_fetcher.requests.get", fake_get)
        df = fetch_catalog("mdb-1847")

        assert offsets_seen == [
            0,
            2,
        ]  # page 1 full (2 rows), page 2 short (1 row) ends it
        assert len(df) == 3

    def test_drops_rows_missing_hash(self, monkeypatch):
        monkeypatch.setenv("MDB_REFRESH_TOKEN", "the-refresh-token")
        monkeypatch.setattr("analysis.gtfs_fetcher.requests.post", _fake_post_token())
        rows = [*SAMPLE_DATASETS, {**SAMPLE_DATASETS[0], "hash": None, "id": "no-hash"}]
        monkeypatch.setattr(
            "analysis.gtfs_fetcher.requests.get",
            lambda *a, **k: FakeResponse(_json=rows),
        )
        df = fetch_catalog("mdb-1847")
        assert len(df) == 3  # the null-hash row dropped


class TestPickSnapshot:
    @pytest.fixture
    def catalog(self) -> pd.DataFrame:
        return pd.DataFrame(
            {
                "feed_start_date": pd.to_datetime(
                    [r["service_date_range_start"] for r in SAMPLE_DATASETS], utc=True
                ),
                "feed_end_date": pd.to_datetime(
                    [r["service_date_range_end"] for r in SAMPLE_DATASETS], utc=True
                ),
                "feed_version": [r["downloaded_at"] for r in SAMPLE_DATASETS],
                "archive_url": [r["hosted_url"] for r in SAMPLE_DATASETS],
                "content_hash": [r["hash"] for r in SAMPLE_DATASETS],
            }
        )

    def test_picks_latest_start_date_within_window(self, catalog):
        # 2026-05-20 is in all three windows; latest start is 05-20 (v3)
        snap = pick_snapshot(catalog, dt.date(2026, 5, 20))
        assert snap.archive_url == "https://example/v3.zip"

    def test_picks_earlier_snapshot_when_latest_doesnt_cover(self, catalog):
        # 2026-05-15 only matches v1 (start=05-14) — v2 starts 05-19, v3 starts 05-20
        snap = pick_snapshot(catalog, dt.date(2026, 5, 15))
        assert snap.archive_url == "https://example/v1.zip"

    def test_raises_when_no_snapshot_covers_date(self, catalog):
        with pytest.raises(LookupError):
            pick_snapshot(catalog, dt.date(2026, 5, 1))

    def test_version_slug_derives_from_content_hash(self, catalog):
        snap = pick_snapshot(catalog, dt.date(2026, 5, 20))
        assert snap.version_slug == "hash-v3"[:16]
        assert snap.content_hash == "hash-v3"

    def test_tie_break_picks_most_recently_crawled_row(self):
        # Same date window (this is exactly GO_AHEAD/TFNSW's real shape:
        # feed_start_date constant, only downloaded_at differs), two
        # different re-crawls with DIFFERENT hashes -- the more recently
        # crawled one must win, not just whichever the API happened to list
        # first (the bug this whole rewrite exists to fix).
        catalog = pd.DataFrame(
            {
                "feed_start_date": pd.to_datetime(
                    ["2019-04-14", "2019-04-14"], utc=True
                ),
                "feed_end_date": pd.to_datetime(["2029-09-01", "2029-09-03"], utc=True),
                "feed_version": [
                    "2026-09-01T00:00:00.000000Z",
                    "2026-09-03T00:00:00.000000Z",
                ],
                "archive_url": ["https://example/old.zip", "https://example/new.zip"],
                "content_hash": ["hash-old", "hash-new"],
            }
        )
        snap = pick_snapshot(catalog, dt.date(2026, 9, 3))
        assert snap.content_hash == "hash-new"

    def test_recrawl_with_unchanged_hash_collapses_to_same_slug(self):
        # The actual bug: a re-crawl of IDENTICAL content used to get a new
        # version_slug every time (timestamp-derived). Two rows, different
        # downloaded_at, same hash -> same version_slug now.
        catalog = pd.DataFrame(
            {
                "feed_start_date": pd.to_datetime(
                    ["2019-04-14", "2019-04-14"], utc=True
                ),
                "feed_end_date": pd.to_datetime(["2029-09-01", "2029-09-02"], utc=True),
                "feed_version": [
                    "2026-09-01T00:00:00.000000Z",
                    "2026-09-02T00:00:00.000000Z",
                ],
                "archive_url": ["https://example/day1.zip", "https://example/day2.zip"],
                "content_hash": ["hash-unchanged", "hash-unchanged"],
            }
        )
        snap_day1 = pick_snapshot(catalog, dt.date(2026, 9, 1))
        snap_day2 = pick_snapshot(catalog, dt.date(2026, 9, 2))
        assert snap_day1.version_slug == snap_day2.version_slug


class TestEnsureLocalZip:
    @pytest.fixture
    def snapshot(self) -> Snapshot:
        return Snapshot(
            feed_start_date=dt.date(2026, 5, 20),
            feed_end_date=dt.date(2026, 9, 7),
            feed_version="2026-05-21T00:57:40Z",
            archive_url="https://example/v3.zip",
            content_hash="hash-v3",
        )

    @pytest.fixture
    def fake_zip_bytes(self) -> bytes:
        buf = io.BytesIO()
        with zipfile.ZipFile(buf, "w") as z:
            z.writestr("trips.txt", "trip_id\nT1\n")
        return buf.getvalue()

    def test_downloads_when_missing(
        self, monkeypatch, tmp_path, snapshot, fake_zip_bytes
    ):
        def fake_get(url, stream=False, timeout=None):
            assert url == snapshot.archive_url
            return FakeResponse(content=fake_zip_bytes)

        monkeypatch.setattr("analysis.gtfs_fetcher.requests.get", fake_get)
        path = ensure_local_zip(snapshot, "wmata", cache_dir=tmp_path)

        assert path == tmp_path / "wmata" / f"v{snapshot.version_slug}" / "feed.zip"
        assert path.exists()
        assert path.read_bytes() == fake_zip_bytes

    def test_skips_when_cached(self, monkeypatch, tmp_path, snapshot, fake_zip_bytes):
        dest = tmp_path / "wmata" / f"v{snapshot.version_slug}" / "feed.zip"
        dest.parent.mkdir(parents=True)
        dest.write_bytes(fake_zip_bytes)

        call_count = {"n": 0}

        def fake_get(*a, **k):
            call_count["n"] += 1
            return FakeResponse(content=b"shouldnt happen")

        monkeypatch.setattr("analysis.gtfs_fetcher.requests.get", fake_get)
        path = ensure_local_zip(snapshot, "wmata", cache_dir=tmp_path)
        assert path == dest
        assert call_count["n"] == 0


class TestGtfsResolver:
    def test_loads_each_snapshot_once_across_dates(self, monkeypatch, tmp_path):
        monkeypatch.setenv("MDB_REFRESH_TOKEN", "the-refresh-token")
        monkeypatch.setattr("analysis.gtfs_fetcher.requests.post", _fake_post_token())

        buf = io.BytesIO()
        with zipfile.ZipFile(buf, "w") as z:
            z.writestr(
                "calendar.txt",
                "service_id,monday,tuesday,wednesday,thursday,friday,saturday,sunday,start_date,end_date\n"
                "WD,1,1,1,1,1,0,0,20260501,20260531\n",
            )
            z.writestr("trips.txt", "trip_id,route_id,service_id,direction_id\n")
            z.writestr(
                "stop_times.txt",
                "trip_id,arrival_time,departure_time,stop_id,stop_sequence\n",
            )
        zip_bytes = buf.getvalue()

        call_counts = {"catalog": 0, "download": 0}

        def fake_get(url, headers=None, params=None, stream=False, timeout=None):
            if "/datasets" in url:
                call_counts["catalog"] += 1
                return FakeResponse(_json=SAMPLE_DATASETS)
            else:
                call_counts["download"] += 1
                return FakeResponse(content=zip_bytes)

        monkeypatch.setattr("analysis.gtfs_fetcher.requests.get", fake_get)
        resolver = GtfsResolver("mdb-1847", "wmata", cache_dir=tmp_path)

        # Two dates that share the same snapshot (both fall in v3's window with latest start 05-20)
        g1 = resolver.for_date(dt.date(2026, 5, 20))
        g2 = resolver.for_date(dt.date(2026, 5, 25))

        assert g1 is g2  # same object — memoized
        assert call_counts["catalog"] == 1  # catalog fetched once
        assert call_counts["download"] == 1  # zip downloaded once
