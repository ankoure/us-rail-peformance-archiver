"""Tests for pipeline/gtfs.py — the static-GTFS normalization marts.

HTTP is monkeypatched (analysis.gtfs_fetcher.requests.get/.post), same style
as tests/test_analysis_gtfs_fetcher.py. No network calls.
"""

from __future__ import annotations

import datetime as dt
import io
import zipfile
from dataclasses import dataclass, field
from pathlib import Path

import pytest
import requests

import analysis.gtfs_fetcher as gtfs_fetcher
from analysis.gtfs_fetcher import GtfsResolver
from analysis.static_gtfs import StaticGtfs
from pipeline import gtfs as pgtfs

# 16+ hex chars so SAMPLE_HASH[:16] (== the real version_slug logic) is
# unambiguous in assertions below.
SAMPLE_HASH = "deadbeef12345678cafefeed"
SAMPLE_VERSION_SLUG = SAMPLE_HASH[:16]

SAMPLE_DATASETS = [
    {
        "id": "mdb-1847-20260521",
        "feed_id": "mdb-1847",
        "hosted_url": "https://example/v3.zip",
        "downloaded_at": "2026-05-21T00:57:40.772601Z",
        "hash": SAMPLE_HASH,
        "service_date_range_start": "2026-05-20T00:00:00Z",
        "service_date_range_end": "2026-09-07T00:00:00Z",
    }
]

TOKEN_RESPONSE = {
    "access_token": "fake-access-token",
    "expiration_datetime_utc": "2099-01-01T00:00:00Z",
    "token_type": "Bearer",
}

SAMPLE_STOPS = (
    "stop_id,stop_code,stop_name,stop_lat,stop_lon\nS1,001,Union Station,38.9,-77.0\n"
)
# route_id differs from route_short_name, mirroring the MBTA SL5 (route_id
# 749, route_short_name SL5) case the route_id_crosswalk table exists for.
SAMPLE_ROUTES = (
    "route_id,route_short_name,route_long_name,route_type\n749,SL5,Silver Line 5,3\n"
)
SAMPLE_CALENDAR = (
    "service_id,monday,tuesday,wednesday,thursday,friday,saturday,sunday,"
    "start_date,end_date\nWD,1,1,1,1,1,0,0,20260501,20260531\n"
)
SAMPLE_CALENDAR_DATES = "service_id,date,exception_type\nWD,20260525,2\n"
SAMPLE_SHAPES = (
    "shape_id,shape_pt_lat,shape_pt_lon,shape_pt_sequence\n"
    "SH1,38.90,-77.00,1\nSH1,38.91,-77.01,2\n"
)
SAMPLE_ROUTE_PATTERNS = (
    "route_pattern_id,route_id,direction_id,route_pattern_name,"
    "route_pattern_typicality,representative_trip_id\n"
    "R-1-0,R,0,Harvard - Nubian,1,T1\n"
)
SAMPLE_DIRECTIONS = (
    "route_id,direction_id,direction,direction_destination\nR,0,Outbound,Nubian\n"
)
SAMPLE_CHECKPOINTS = "checkpoint_id,checkpoint_name\nHARSQ,Harvard Square\n"

CONFIG_YAML = """
writer:
  landing_dir: ./landing
  curated_dir: ./curated
agencies:
  - agency_id: WMATA
    name: WMATA
    region: DC
    timezone: America/New_York
    base_url: https://example.com
    mdb_feed_id: mdb-1847
    auth:
      type: none
    feeds:
      - name: wmata-vehicles
        path: /vehicles
  - agency_id: NOMDB
    name: No MDB Agency
    region: Nowhere
    timezone: America/New_York
    base_url: https://example.com
    auth:
      type: none
    feeds:
      - name: nomdb-vehicles
        path: /vehicles
"""


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
    """Module-level token cache is shared global state -- reset between tests."""
    gtfs_fetcher._reset_token_cache()
    yield
    gtfs_fetcher._reset_token_cache()


def _build_zip_bytes(
    stops: str | None = None,
    routes: str | None = None,
    calendar: str | None = None,
    calendar_dates: str | None = None,
    shapes: str | None = None,
    route_patterns: str | None = None,
    directions: str | None = None,
    checkpoints: str | None = None,
) -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as z:
        if stops is not None:
            z.writestr("stops.txt", stops)
        if routes is not None:
            z.writestr("routes.txt", routes)
        if calendar is not None:
            z.writestr("calendar.txt", calendar)
        if calendar_dates is not None:
            z.writestr("calendar_dates.txt", calendar_dates)
        if shapes is not None:
            z.writestr("shapes.txt", shapes)
        if route_patterns is not None:
            z.writestr("route_patterns.txt", route_patterns)
        if directions is not None:
            z.writestr("directions.txt", directions)
        if checkpoints is not None:
            z.writestr("checkpoints.txt", checkpoints)
    return buf.getvalue()


def _patch_gtfs_http(
    monkeypatch, zip_bytes: bytes, datasets: list[dict] = SAMPLE_DATASETS
):
    monkeypatch.setenv("MDB_REFRESH_TOKEN", "the-refresh-token")
    call_counts = {"catalog": 0, "download": 0}

    def fake_post(url, json=None, timeout=None):
        return FakeResponse(_json=TOKEN_RESPONSE)

    def fake_get(url, headers=None, params=None, stream=False, timeout=None):
        if "/datasets" in url:
            call_counts["catalog"] += 1
            return FakeResponse(_json=datasets)
        call_counts["download"] += 1
        return FakeResponse(content=zip_bytes)

    monkeypatch.setattr("analysis.gtfs_fetcher.requests.post", fake_post)
    monkeypatch.setattr("analysis.gtfs_fetcher.requests.get", fake_get)
    return call_counts


@pytest.fixture
def config_path(tmp_path) -> Path:
    p = tmp_path / "feeds.yaml"
    p.write_text(CONFIG_YAML)
    return p


class TestRowBuilders:
    """Pure row-builder functions given a StaticGtfs — no resolver/CLI plumbing."""

    def _gtfs(self, tmp_path: Path) -> StaticGtfs:
        zp = tmp_path / "feed.zip"
        zp.write_bytes(
            _build_zip_bytes(
                stops=SAMPLE_STOPS,
                routes=SAMPLE_ROUTES,
                calendar=SAMPLE_CALENDAR,
                calendar_dates=SAMPLE_CALENDAR_DATES,
                shapes=SAMPLE_SHAPES,
                route_patterns=SAMPLE_ROUTE_PATTERNS,
                directions=SAMPLE_DIRECTIONS,
                checkpoints=SAMPLE_CHECKPOINTS,
            )
        )
        return StaticGtfs(zp)

    def test_stops_rows(self, tmp_path):
        rows = pgtfs._stops_rows(self._gtfs(tmp_path), "v1")
        assert rows == [
            {
                "stop_id": "S1",
                "stop_code": "001",
                "stop_name": "Union Station",
                "stop_lat": 38.9,
                "stop_lon": -77.0,
                "parent_station": None,
                "version_slug": "v1",
            }
        ]

    def test_routes_rows(self, tmp_path):
        rows = pgtfs._routes_rows(self._gtfs(tmp_path), "v1")
        assert rows == [
            {
                "route_id": "749",
                "route_short_name": "SL5",
                "route_long_name": "Silver Line 5",
                "mode": "bus",
                "version_slug": "v1",
            }
        ]

    def test_route_aliases_rows(self, tmp_path):
        rows = pgtfs._route_aliases_rows(self._gtfs(tmp_path), "v1")
        assert rows == [
            {
                "alias_token": "SL5",
                "alias_type": "short_name",
                "route_id": "749",
                "version_slug": "v1",
            },
            {
                "alias_token": "Silver Line 5",
                "alias_type": "long_name",
                "route_id": "749",
                "version_slug": "v1",
            },
        ]

    def test_calendar_rows(self, tmp_path):
        rows = pgtfs._calendar_rows(self._gtfs(tmp_path), "v1")
        assert len(rows) == 1
        row = rows[0]
        assert row["service_id"] == "WD"
        assert row["monday"] is True
        assert row["saturday"] is False
        assert row["start_date"] == "2026-05-01"
        assert row["end_date"] == "2026-05-31"
        assert row["version_slug"] == "v1"

    def test_calendar_dates_rows(self, tmp_path):
        rows = pgtfs._calendar_dates_rows(self._gtfs(tmp_path), "v1")
        assert rows == [
            {
                "service_id": "WD",
                "date": "2026-05-25",
                "exception_type": 2,
                "version_slug": "v1",
            }
        ]

    def test_shapes_rows(self, tmp_path):
        rows = pgtfs._shapes_rows(self._gtfs(tmp_path), "v1")
        assert [r["shape_pt_sequence"] for r in rows] == [1, 2]
        assert rows[0]["shape_id"] == "SH1"
        assert rows[0]["shape_dist_traveled"] is None

    def test_route_patterns_rows(self, tmp_path):
        rows = pgtfs._route_patterns_rows(self._gtfs(tmp_path), "v1")
        assert rows == [
            {
                "route_pattern_id": "R-1-0",
                "route_id": "R",
                "direction_id": 0,
                "route_pattern_name": "Harvard - Nubian",
                "route_pattern_typicality": 1,
                "representative_trip_id": "T1",
                "version_slug": "v1",
            }
        ]

    def test_directions_rows(self, tmp_path):
        rows = pgtfs._directions_rows(self._gtfs(tmp_path), "v1")
        assert rows == [
            {
                "route_id": "R",
                "direction_id": 0,
                "direction": "Outbound",
                "direction_destination": "Nubian",
                "version_slug": "v1",
            }
        ]

    def test_checkpoints_rows(self, tmp_path):
        rows = pgtfs._checkpoints_rows(self._gtfs(tmp_path), "v1")
        assert rows == [
            {
                "checkpoint_id": "HARSQ",
                "checkpoint_name": "Harvard Square",
                "version_slug": "v1",
            }
        ]

    def test_empty_when_optional_files_absent(self, tmp_path):
        zp = tmp_path / "feed.zip"
        zp.write_bytes(_build_zip_bytes())  # no tables at all
        gtfs = StaticGtfs(zp)
        assert pgtfs._stops_rows(gtfs, "v1") == []
        assert pgtfs._routes_rows(gtfs, "v1") == []
        assert pgtfs._route_aliases_rows(gtfs, "v1") == []
        assert pgtfs._calendar_rows(gtfs, "v1") == []
        assert pgtfs._calendar_dates_rows(gtfs, "v1") == []
        assert pgtfs._shapes_rows(gtfs, "v1") == []
        assert pgtfs._route_patterns_rows(gtfs, "v1") == []
        assert pgtfs._directions_rows(gtfs, "v1") == []
        assert pgtfs._checkpoints_rows(gtfs, "v1") == []


class TestProcessFeedDay:
    def test_builds_version_marts_and_manifest(self, tmp_path, monkeypatch):
        zip_bytes = _build_zip_bytes(
            stops=SAMPLE_STOPS,
            routes=SAMPLE_ROUTES,
            calendar=SAMPLE_CALENDAR,
            shapes=SAMPLE_SHAPES,
            route_patterns=SAMPLE_ROUTE_PATTERNS,
            directions=SAMPLE_DIRECTIONS,
            checkpoints=SAMPLE_CHECKPOINTS,
        )
        _patch_gtfs_http(monkeypatch, zip_bytes)
        resolver = GtfsResolver("mdb-1847", "wmata", cache_dir=tmp_path / "cache")
        curated = tmp_path / "curated"
        written: set[tuple[str, str]] = set()

        result = pgtfs.process_feed_day(
            "wmata-vehicles", dt.date(2026, 5, 20), resolver, curated, False, written
        )
        assert result == {
            "gtfs_stops": 1,
            "gtfs_routes": 1,
            "gtfs_route_aliases": 2,
            "gtfs_calendar": 1,
            "gtfs_calendar_dates": 0,
            "gtfs_shapes": 2,
            "gtfs_route_patterns": 1,
            "gtfs_directions": 1,
            "gtfs_checkpoints": 1,
            "route_shapes": 0,
            "route_shape_stops": 0,
            "manifest": 1,
        }
        assert len(written) == 1
        version_slug = next(iter(written))[1]
        assert version_slug == SAMPLE_VERSION_SLUG

        stops_path = pgtfs._version_partition_path(
            curated, "gtfs_stops", "wmata-vehicles", version_slug
        )
        calendar_dates_path = pgtfs._version_partition_path(
            curated, "gtfs_calendar_dates", "wmata-vehicles", version_slug
        )
        manifest_path = pgtfs._mart_path(
            curated, "gtfs_versions", "wmata-vehicles", dt.date(2026, 5, 20)
        )
        assert stops_path.exists()
        # Written even though empty, so the idempotency check below doesn't
        # re-fetch forever for a feed with no calendar_dates.txt.
        assert calendar_dates_path.exists()
        assert manifest_path.exists()

    def test_second_day_same_version_skips_rebuild(self, tmp_path, monkeypatch):
        zip_bytes = _build_zip_bytes(stops=SAMPLE_STOPS)
        call_counts = _patch_gtfs_http(monkeypatch, zip_bytes)
        resolver = GtfsResolver("mdb-1847", "wmata", cache_dir=tmp_path / "cache")
        curated = tmp_path / "curated"
        written: set[tuple[str, str]] = set()

        pgtfs.process_feed_day(
            "wmata-vehicles", dt.date(2026, 5, 20), resolver, curated, False, written
        )
        assert call_counts["download"] == 1

        result2 = pgtfs.process_feed_day(
            "wmata-vehicles", dt.date(2026, 5, 21), resolver, curated, False, written
        )
        assert call_counts["download"] == 1  # no second parse/download
        assert result2["gtfs_stops"] == 0  # version marts not rewritten
        assert result2["manifest"] == 1  # but a new day still gets its own row

        manifest_path_1 = pgtfs._mart_path(
            curated, "gtfs_versions", "wmata-vehicles", dt.date(2026, 5, 20)
        )
        manifest_path_2 = pgtfs._mart_path(
            curated, "gtfs_versions", "wmata-vehicles", dt.date(2026, 5, 21)
        )
        assert manifest_path_1.exists()
        assert manifest_path_2.exists()

    def test_recrawl_with_unchanged_hash_skips_rebuild(self, tmp_path, monkeypatch):
        """The actual bug this whole module was rewritten to fix, modeled as
        faithfully as a unit test can: two SEPARATE resolvers (matching
        production, where pipeline/gtfs.py's main() builds a fresh
        GtfsResolver -- and so does a fresh catalog fetch -- every run, not
        just a fresh `for_date` call), simulating two different nights' runs.
        Night 2's catalog has one MORE row than night 1's (a new crawl, later
        downloaded_at) but that new row's hash is IDENTICAL to night 1's
        picked snapshot -- night 2 must not re-download/re-parse. Before
        2026-09-06 this never happened -- every crawl got a new
        timestamp-derived slug regardless of content."""
        zip_bytes = _build_zip_bytes(stops=SAMPLE_STOPS)
        night2_datasets = [
            {
                **SAMPLE_DATASETS[0],
                "id": "mdb-1847-recrawl",
                "downloaded_at": "2026-05-22T00:00:00.000000Z",
            },
            SAMPLE_DATASETS[0],
        ]
        curated = tmp_path / "curated"

        call_counts_1 = _patch_gtfs_http(
            monkeypatch, zip_bytes, datasets=SAMPLE_DATASETS
        )
        resolver_night1 = GtfsResolver(
            "mdb-1847", "wmata", cache_dir=tmp_path / "cache1"
        )
        pgtfs.process_feed_day(
            "wmata-vehicles",
            dt.date(2026, 5, 20),
            resolver_night1,
            curated,
            False,
            set(),
        )
        assert call_counts_1["download"] == 1

        call_counts_2 = _patch_gtfs_http(
            monkeypatch, zip_bytes, datasets=night2_datasets
        )
        resolver_night2 = GtfsResolver(
            "mdb-1847", "wmata", cache_dir=tmp_path / "cache2"
        )
        result2 = pgtfs.process_feed_day(
            "wmata-vehicles",
            dt.date(2026, 5, 22),
            resolver_night2,
            curated,
            False,
            set(),
        )
        assert call_counts_2["download"] == 0  # unchanged hash -> no download at all
        assert result2["gtfs_stops"] == 0  # version marts not rewritten

    def test_recrawl_with_changed_hash_rebuilds(self, tmp_path, monkeypatch):
        """The complementary case: night 2's re-crawl has a genuinely
        different hash -- a real content change -- and must still trigger a
        full rebuild. The one behavior that's easy to accidentally regress
        toward "never rebuilds" while fixing the bug above."""
        zip_bytes = _build_zip_bytes(stops=SAMPLE_STOPS)
        night2_datasets = [
            {
                **SAMPLE_DATASETS[0],
                "id": "mdb-1847-changed",
                "downloaded_at": "2026-05-22T00:00:00.000000Z",
                "hash": "newhash0000000000000000",
            },
            SAMPLE_DATASETS[0],
        ]
        curated = tmp_path / "curated"

        call_counts_1 = _patch_gtfs_http(
            monkeypatch, zip_bytes, datasets=SAMPLE_DATASETS
        )
        resolver_night1 = GtfsResolver(
            "mdb-1847", "wmata", cache_dir=tmp_path / "cache1"
        )
        pgtfs.process_feed_day(
            "wmata-vehicles",
            dt.date(2026, 5, 20),
            resolver_night1,
            curated,
            False,
            set(),
        )
        assert call_counts_1["download"] == 1

        call_counts_2 = _patch_gtfs_http(
            monkeypatch, zip_bytes, datasets=night2_datasets
        )
        resolver_night2 = GtfsResolver(
            "mdb-1847", "wmata", cache_dir=tmp_path / "cache2"
        )
        result2 = pgtfs.process_feed_day(
            "wmata-vehicles",
            dt.date(2026, 5, 22),
            resolver_night2,
            curated,
            False,
            set(),
        )
        assert call_counts_2["download"] == 1  # changed hash -> real rebuild
        assert result2["gtfs_stops"] == 1

    def test_force_rebuilds_version_marts(self, tmp_path, monkeypatch):
        zip_bytes = _build_zip_bytes(stops=SAMPLE_STOPS)
        _patch_gtfs_http(monkeypatch, zip_bytes)
        resolver = GtfsResolver("mdb-1847", "wmata", cache_dir=tmp_path / "cache")
        curated = tmp_path / "curated"
        pgtfs.process_feed_day(
            "wmata-vehicles", dt.date(2026, 5, 20), resolver, curated, False, set()
        )

        # A fresh run (new written_versions set) with existing files on disk
        # and --force set must rebuild rather than skip.
        result = pgtfs.process_feed_day(
            "wmata-vehicles", dt.date(2026, 5, 20), resolver, curated, True, set()
        )
        assert result["gtfs_stops"] == 1

    def test_no_snapshot_covers_day_returns_none(self, tmp_path, monkeypatch):
        zip_bytes = _build_zip_bytes(stops=SAMPLE_STOPS)
        _patch_gtfs_http(monkeypatch, zip_bytes)
        resolver = GtfsResolver("mdb-1847", "wmata", cache_dir=tmp_path / "cache")
        curated = tmp_path / "curated"

        result = pgtfs.process_feed_day(
            "wmata-vehicles", dt.date(2020, 1, 1), resolver, curated, False, set()
        )
        assert result is None


class TestMain:
    def test_no_mdb_feed_id_clean_skip(self, tmp_path, config_path, capsys):
        rc = pgtfs.main(
            [
                "--feed",
                "nomdb-vehicles",
                "--day",
                "2026-05-20",
                "-c",
                str(config_path),
                "--curated-dir",
                str(tmp_path / "curated"),
            ]
        )
        assert rc == 0
        assert "no mdb_feed_id" in capsys.readouterr().err

    def test_catalog_failure_skips_agency_not_whole_run(
        self, tmp_path, config_path, monkeypatch, capsys
    ):
        monkeypatch.setenv("MDB_REFRESH_TOKEN", "the-refresh-token")

        def fake_post(url, json=None, timeout=None):
            return FakeResponse(_json=TOKEN_RESPONSE)

        def fake_get(url, headers=None, params=None, stream=False, timeout=None):
            raise requests.exceptions.ConnectionError("boom")

        monkeypatch.setattr("analysis.gtfs_fetcher.requests.post", fake_post)
        monkeypatch.setattr("analysis.gtfs_fetcher.requests.get", fake_get)

        rc = pgtfs.main(
            [
                "--feed",
                "wmata-vehicles",
                "--day",
                "2026-05-20",
                "-c",
                str(config_path),
                "--curated-dir",
                str(tmp_path / "curated"),
                "--gtfs-cache-dir",
                str(tmp_path / "cache"),
            ]
        )
        assert rc == 0  # a bad agency must not abort the run / break `&&` chains
        assert "GTFS catalog/zip unavailable" in capsys.readouterr().err

    def test_end_to_end_writes_marts(self, tmp_path, config_path, monkeypatch):
        zip_bytes = _build_zip_bytes(stops=SAMPLE_STOPS, calendar=SAMPLE_CALENDAR)
        _patch_gtfs_http(monkeypatch, zip_bytes)
        curated = tmp_path / "curated"

        rc = pgtfs.main(
            [
                "--feed",
                "wmata-vehicles",
                "--day",
                "2026-05-20",
                "-c",
                str(config_path),
                "--curated-dir",
                str(curated),
                "--gtfs-cache-dir",
                str(tmp_path / "cache"),
            ]
        )
        assert rc == 0
        manifest_path = pgtfs._mart_path(
            curated, "gtfs_versions", "wmata-vehicles", dt.date(2026, 5, 20)
        )
        stops_path = pgtfs._version_partition_path(
            curated, "gtfs_stops", "wmata-vehicles", SAMPLE_VERSION_SLUG
        )
        assert manifest_path.exists()
        assert stops_path.exists()
