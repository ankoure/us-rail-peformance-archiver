"""Build normalized static-GTFS reference marts from each agency's schedule snapshot.

Unlike every other gold-tier mart, these are NOT partitioned per day — a GTFS
static snapshot is valid for weeks or months, so partitioning by day the way
`gold.py`'s `routes` mart does would duplicate the same rows every day for no
reason (and this feed set already learned that lesson the hard way with S3
landing-zone storage). Instead:

    {curated}/metrics/gtfs_stops/feed={feed}/version={version_slug}/data.parquet
    {curated}/metrics/gtfs_routes/feed={feed}/version={version_slug}/data.parquet
    {curated}/metrics/gtfs_route_aliases/feed={feed}/version={version_slug}/data.parquet
    {curated}/metrics/gtfs_calendar/feed={feed}/version={version_slug}/data.parquet
    {curated}/metrics/gtfs_calendar_dates/feed={feed}/version={version_slug}/data.parquet
    {curated}/metrics/gtfs_shapes/feed={feed}/version={version_slug}/data.parquet
    {curated}/metrics/route_shapes/feed={feed}/version={version_slug}/data.parquet
    {curated}/metrics/route_shape_stops/feed={feed}/version={version_slug}/data.parquet

are written exactly once per (feed, snapshot version) regardless of how many
days that snapshot is in effect. `version_slug` comes from
`analysis.gtfs_fetcher.Snapshot.version_slug`, the same key GtfsResolver uses
to cache the downloaded zip.

A caller who thinks in terms of "what schedule was in effect on day D" (the
normal way every other mart is addressed) resolves the version through a
small day-partitioned manifest mart instead:

    {curated}/metrics/gtfs_versions/feed={feed}/year=/month=/day=/data.parquet

Also normalizes three MBTA GTFS extension files, version-partitioned the same
way (route_patterns.txt, directions.txt, checkpoints.txt) — optional on
nearly every non-MBTA feed, degrading to an empty-but-present mart exactly
like stops/calendar/shapes do when the source file is absent:

    {curated}/metrics/gtfs_route_patterns/feed={feed}/version={version_slug}/data.parquet
    {curated}/metrics/gtfs_directions/feed={feed}/version={version_slug}/data.parquet
    {curated}/metrics/gtfs_checkpoints/feed={feed}/version={version_slug}/data.parquet

checkpoints is a reference table only (checkpoint_id -> checkpoint_name) —
it does not derive a stop_id -> checkpoint_id mapping, since MBTA's
checkpoint_id lives on stop_times.txt (per stop_time, i.e. per trip), not on
stops.txt, and isn't guaranteed stable per stop. `StaticGtfs.stop_times`
exposes `checkpoint_id` directly (when present) for a caller who wants that
join themselves.

route_shapes / route_shape_stops derive one canonical polyline per
route+direction (see `StaticGtfs.route_direction_shapes`/
`route_direction_stop_offsets`) for callers that want to draw a route on a
map — route_shapes is the ordered polyline with cumulative arc-length
`dist_m`, route_shape_stops is each stop's projected `dist_m` along it, so a
caller can slice the polyline between any two stops without redoing the
shape-projection math itself.

gtfs_stops carries `parent_station` (grouping direction-dependent platforms
under one physical station — see docs/design/stop-id-stability-findings.md).
gtfs_routes is routes.txt typed and tagged with `mode` (see
StaticGtfs.route_modes); it's version-partitioned rather than the
day-partitioned `routes` mart gold.py builds, since a PostGIS consumer wants
the same (feed, version_slug) grain as the other marts here. gtfs_route_aliases
is one row per non-null route_short_name/route_long_name, tagged
`alias_type` — a raw display-token -> route_id crosswalk (e.g. MBTA's SL5
short name -> its real route_id 749), left as full rows rather than
StaticGtfs.route_short_names' keep-first dict so a downstream loader can
apply its own collision precedence.

Calendar ships as a typed passthrough of calendar.txt + calendar_dates.txt,
not a resolved service_id-per-date expansion — that join is cheap, in-memory
work a reader can do itself (see analysis.static_gtfs.StaticGtfs.active_service_ids),
and precomputing it would reintroduce day-linear row growth inside a
version-partitioned mart, defeating the point of moving off daily partitions.

This mirrors gold.py's CLI shell (argparse, --day/--all-days/--force) and
self-skip contract (missing mdb_feed_id, unresolvable snapshot, or a failed
catalog fetch skips just that feed/agency — never aborts the run) but is a
separate script because its idempotency unit (a snapshot version, spanning
many days) doesn't fit gold.py's per-(feed, day) mart contract.

Known redundancy, not addressed here: gold.py's own GTFS resolver will
independently re-parse the same snapshot zip for OTP/routes. The on-disk zip
cache avoids a second download, but not a second CSV parse, since StaticGtfs
instances aren't shared across processes. See docs/design/static-gtfs-normalization.md.

Examples:

    # one feed, one day (resolves + writes that day's schedule snapshot if new)
    uv run python pipeline/gtfs.py --feed wmata-vehicles --day 2026-05-20

    # every feed with an mdb_feed_id, every day already on disk
    uv run python pipeline/gtfs.py --all-days

    # force-rebuild the version marts even if already written
    uv run python pipeline/gtfs.py --feed wmata-vehicles --day 2026-05-20 --force
"""

from __future__ import annotations

import argparse
import datetime as dt
import re
import sys
from collections import Counter
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import requests
from dotenv import load_dotenv

# Make the repo root importable when run as `python pipeline/gtfs.py`.
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from analysis.geo import cumulative_arc_length_m  # noqa: E402
from analysis.gtfs_fetcher import GtfsResolver, pick_snapshot  # noqa: E402
from pipeline.agency_map import (  # noqa: E402
    load_feed_agency_map,
    load_feed_gtfs_scope,
)

load_dotenv()

_PART_RE = re.compile(r"^(year|month|day)=(\d+)$")
_SOURCE_SUBDIR = {"vehicles": "vehicles", "trip-updates": "trip_updates"}

_VERSION_MARTS = (
    "gtfs_stops",
    "gtfs_routes",
    "gtfs_route_aliases",
    "gtfs_calendar",
    "gtfs_calendar_dates",
    "gtfs_shapes",
    "gtfs_route_patterns",
    "gtfs_directions",
    "gtfs_checkpoints",
    "route_shapes",
    "route_shape_stops",
)
_VERSIONS_MART = "gtfs_versions"

_WEEKDAY_COLS = [
    "monday",
    "tuesday",
    "wednesday",
    "thursday",
    "friday",
    "saturday",
    "sunday",
]

_DEFAULT_GTFS_API_URL = "https://mwue7uiyf5.execute-api.us-east-1.amazonaws.com/api"
_DEFAULT_GTFS_CACHE_DIR = Path("data/static_gtfs")

GTFS_VERSIONS_SCHEMA = pa.schema(
    [
        pa.field("service_date", pa.string(), nullable=False),
        pa.field("version_slug", pa.string(), nullable=False),
        pa.field("feed_version", pa.string(), nullable=True),
        pa.field("feed_start_date", pa.string(), nullable=False),
        pa.field("feed_end_date", pa.string(), nullable=False),
    ]
)

GTFS_STOPS_SCHEMA = pa.schema(
    [
        pa.field("stop_id", pa.string(), nullable=False),
        pa.field("stop_code", pa.string(), nullable=True),
        pa.field("stop_name", pa.string(), nullable=True),
        pa.field("stop_lat", pa.float64(), nullable=True),
        pa.field("stop_lon", pa.float64(), nullable=True),
        pa.field("parent_station", pa.string(), nullable=True),
        pa.field("version_slug", pa.string(), nullable=False),
    ]
)

GTFS_ROUTES_SCHEMA = pa.schema(
    [
        pa.field("route_id", pa.string(), nullable=False),
        pa.field("route_short_name", pa.string(), nullable=True),
        pa.field("route_long_name", pa.string(), nullable=True),
        pa.field("mode", pa.string(), nullable=False),
        pa.field("version_slug", pa.string(), nullable=False),
    ]
)

# One row per non-null route_short_name / route_long_name in routes.txt,
# tagged with which column it came from — feeds the PostGIS route_id
# crosswalk (raw display token -> canonical route_id). Kept as full rows
# rather than StaticGtfs.route_short_names' keep-first dict so a downstream
# loader can apply its own keep-first precedence (e.g. via ON CONFLICT DO
# NOTHING) instead of losing collisions here.
GTFS_ROUTE_ALIASES_SCHEMA = pa.schema(
    [
        pa.field("alias_token", pa.string(), nullable=False),
        pa.field("alias_type", pa.string(), nullable=False),  # short_name|long_name
        pa.field("route_id", pa.string(), nullable=False),
        pa.field("version_slug", pa.string(), nullable=False),
    ]
)

GTFS_SHAPES_SCHEMA = pa.schema(
    [
        pa.field("shape_id", pa.string(), nullable=False),
        pa.field("shape_pt_sequence", pa.int32(), nullable=False),
        pa.field("shape_pt_lat", pa.float64(), nullable=False),
        pa.field("shape_pt_lon", pa.float64(), nullable=False),
        pa.field("shape_dist_traveled", pa.float64(), nullable=True),
        pa.field("version_slug", pa.string(), nullable=False),
    ]
)

GTFS_CALENDAR_SCHEMA = pa.schema(
    [
        pa.field("service_id", pa.string(), nullable=False),
        pa.field("monday", pa.bool_(), nullable=False),
        pa.field("tuesday", pa.bool_(), nullable=False),
        pa.field("wednesday", pa.bool_(), nullable=False),
        pa.field("thursday", pa.bool_(), nullable=False),
        pa.field("friday", pa.bool_(), nullable=False),
        pa.field("saturday", pa.bool_(), nullable=False),
        pa.field("sunday", pa.bool_(), nullable=False),
        pa.field("start_date", pa.string(), nullable=False),
        pa.field("end_date", pa.string(), nullable=False),
        pa.field("version_slug", pa.string(), nullable=False),
    ]
)

GTFS_CALENDAR_DATES_SCHEMA = pa.schema(
    [
        pa.field("service_id", pa.string(), nullable=False),
        pa.field("date", pa.string(), nullable=False),
        pa.field("exception_type", pa.int8(), nullable=False),  # 1=added, 2=removed
        pa.field("version_slug", pa.string(), nullable=False),
    ]
)

# MBTA GTFS extensions — optional on nearly every non-MBTA feed.

GTFS_ROUTE_PATTERNS_SCHEMA = pa.schema(
    [
        pa.field("route_pattern_id", pa.string(), nullable=False),
        pa.field("route_id", pa.string(), nullable=False),
        pa.field("direction_id", pa.int8(), nullable=False),
        pa.field("route_pattern_name", pa.string(), nullable=True),
        # 0=not defined, 1=typical, 2=deviation, 3=atypical, 4=diversion
        pa.field("route_pattern_typicality", pa.int8(), nullable=True),
        pa.field("representative_trip_id", pa.string(), nullable=True),
        pa.field("version_slug", pa.string(), nullable=False),
    ]
)

GTFS_DIRECTIONS_SCHEMA = pa.schema(
    [
        pa.field("route_id", pa.string(), nullable=False),
        pa.field("direction_id", pa.int8(), nullable=False),
        pa.field("direction", pa.string(), nullable=True),
        pa.field("direction_destination", pa.string(), nullable=True),
        pa.field("version_slug", pa.string(), nullable=False),
    ]
)

GTFS_CHECKPOINTS_SCHEMA = pa.schema(
    [
        pa.field("checkpoint_id", pa.string(), nullable=False),
        pa.field("checkpoint_name", pa.string(), nullable=True),
        pa.field("version_slug", pa.string(), nullable=False),
    ]
)

# Every shape a route+direction uses (see StaticGtfs.route_direction_shapes —
# a branching line like MBTA Red Line's Ashmont/Braintree split has more than
# one shape per direction_id) and each stop's projected position along each
# of those shapes — for drawing a route+direction's full branch structure on
# a map and slicing between any two stops without redoing the projection.

ROUTE_SHAPES_SCHEMA = pa.schema(
    [
        pa.field("route_id", pa.string(), nullable=False),
        pa.field("direction_id", pa.int8(), nullable=False),
        pa.field("shape_id", pa.string(), nullable=False),
        pa.field("point_sequence", pa.int32(), nullable=False),
        pa.field("lat", pa.float64(), nullable=False),
        pa.field("lon", pa.float64(), nullable=False),
        pa.field("dist_m", pa.float64(), nullable=False),
        pa.field("version_slug", pa.string(), nullable=False),
    ]
)

ROUTE_SHAPE_STOPS_SCHEMA = pa.schema(
    [
        pa.field("route_id", pa.string(), nullable=False),
        pa.field("direction_id", pa.int8(), nullable=False),
        pa.field("shape_id", pa.string(), nullable=False),
        pa.field("stop_id", pa.string(), nullable=False),
        pa.field("dist_m", pa.float64(), nullable=False),
        pa.field("version_slug", pa.string(), nullable=False),
    ]
)

_MART_SCHEMAS = {
    "gtfs_stops": GTFS_STOPS_SCHEMA,
    "gtfs_routes": GTFS_ROUTES_SCHEMA,
    "gtfs_route_aliases": GTFS_ROUTE_ALIASES_SCHEMA,
    "gtfs_calendar": GTFS_CALENDAR_SCHEMA,
    "gtfs_calendar_dates": GTFS_CALENDAR_DATES_SCHEMA,
    "gtfs_shapes": GTFS_SHAPES_SCHEMA,
    "gtfs_route_patterns": GTFS_ROUTE_PATTERNS_SCHEMA,
    "gtfs_directions": GTFS_DIRECTIONS_SCHEMA,
    "gtfs_checkpoints": GTFS_CHECKPOINTS_SCHEMA,
    "route_shapes": ROUTE_SHAPES_SCHEMA,
    "route_shape_stops": ROUTE_SHAPE_STOPS_SCHEMA,
}


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    p.add_argument(
        "--feed",
        nargs="+",
        default=None,
        help="One or more feed names. Omit to process every feed whose agency "
        "has an mdb_feed_id.",
    )
    p.add_argument(
        "--day",
        type=dt.date.fromisoformat,
        help="A single day (YYYY-MM-DD) to resolve the schedule snapshot for.",
    )
    p.add_argument(
        "--all-days",
        action="store_true",
        help="Process every service_date partition on disk for each feed "
        "(unioned with --day).",
    )
    p.add_argument(
        "--source",
        choices=["vehicles", "trip-updates"],
        default="vehicles",
        help="Which curated dataset to discover days from for --all-days "
        "(default: vehicles).",
    )
    p.add_argument(
        "--curated-dir",
        type=Path,
        default=Path("data/curated"),
        help="Curated root: write the gtfs_* marts under here.",
    )
    p.add_argument(
        "-c",
        "--config",
        type=Path,
        default=Path("config/feeds.yaml"),
        help="Path to feeds.yaml (feed -> agency/mdb_feed_id lookup).",
    )
    p.add_argument(
        "--gtfs-cache-dir",
        type=Path,
        default=_DEFAULT_GTFS_CACHE_DIR,
        help=f"Where fetched static GTFS zips are cached (default: "
        f"{_DEFAULT_GTFS_CACHE_DIR}).",
    )
    p.add_argument(
        "--gtfs-api-url",
        default=_DEFAULT_GTFS_API_URL,
        help="Archived-feeds catalog base URL for resolving GTFS snapshots.",
    )
    p.add_argument(
        "-f",
        "--force",
        action="store_true",
        help="Rebuild the version marts even if already written, and rewrite "
        "existing manifest rows.",
    )
    args = p.parse_args(argv)
    if not args.day and not args.all_days:
        p.error("provide --day YYYY-MM-DD or --all-days")
    return args


def discover_dates_for_dir(feed: str, root: Path) -> list[dt.date]:
    """Every service_date partition under `root/feed={feed}` (any curated table)."""
    feed_root = root / f"feed={feed}"
    if not feed_root.exists():
        return []
    found: list[dt.date] = []
    for day_dir in feed_root.glob("year=*/month=*/day=*"):
        parts: dict[str, int] = {}
        for part in day_dir.parts[-3:]:
            m = _PART_RE.match(part)
            if m:
                parts[m.group(1)] = int(m.group(2))
        if set(parts) != {"year", "month", "day"}:
            continue
        try:
            found.append(dt.date(parts["year"], parts["month"], parts["day"]))
        except ValueError:
            continue
    return sorted(set(found))


def discover_dates(feed: str, curated_dir: Path, source: str) -> list[dt.date]:
    return discover_dates_for_dir(feed, curated_dir / _SOURCE_SUBDIR[source])


def _partition_path(root: Path, feed: str, day: dt.date) -> Path:
    return (
        root
        / f"feed={feed}"
        / f"year={day.year}"
        / f"month={day.month}"
        / f"day={day.day}"
        / "data.parquet"
    )


def _mart_path(curated_dir: Path, mart: str, feed: str, day: dt.date) -> Path:
    return _partition_path(curated_dir / "metrics" / mart, feed, day)


def _version_partition_path(
    curated_dir: Path, mart: str, feed: str, version_slug: str
) -> Path:
    return (
        curated_dir
        / "metrics"
        / mart
        / f"feed={feed}"
        / f"version={version_slug}"
        / "data.parquet"
    )


def _write_parquet(rows: list[dict], schema: pa.Schema, path: Path) -> None:
    """Atomic write via tmp + rename (mirrors gold.py's helper).

    Always writes, even for an empty `rows` — an empty-but-present file is
    what marks a version mart as "handled" for a feed that legitimately has
    no calendar_dates.txt / no shapes.txt, so the idempotency check in
    `process_feed_day` doesn't re-fetch and re-parse that snapshot forever.
    """
    table = pa.Table.from_pylist(rows, schema=schema)
    tmp = path.with_suffix(".parquet.tmp")
    tmp.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(table, tmp)
    tmp.rename(path)


def _iso(value) -> str | None:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    if hasattr(value, "date"):
        return value.date().isoformat()
    return str(value)


def _stops_rows(gtfs, version_slug: str) -> list[dict]:
    rows = []
    for row in gtfs.stops.itertuples(index=False):
        stop_id = getattr(row, "stop_id", None)
        if not isinstance(stop_id, str) or not stop_id:
            continue
        stop_code = getattr(row, "stop_code", None)
        stop_name = getattr(row, "stop_name", None)
        lat = getattr(row, "stop_lat", None)
        lon = getattr(row, "stop_lon", None)
        parent_station = getattr(row, "parent_station", None)
        rows.append(
            {
                "stop_id": stop_id,
                "stop_code": stop_code if isinstance(stop_code, str) else None,
                "stop_name": stop_name if isinstance(stop_name, str) else None,
                "stop_lat": float(lat) if pd.notna(lat) else None,
                "stop_lon": float(lon) if pd.notna(lon) else None,
                "parent_station": (
                    parent_station if isinstance(parent_station, str) else None
                ),
                "version_slug": version_slug,
            }
        )
    return rows


def _routes_rows(gtfs, version_slug: str) -> list[dict]:
    modes = gtfs.route_modes
    rows = []
    for row in gtfs.routes.itertuples(index=False):
        route_id = getattr(row, "route_id", None)
        if not isinstance(route_id, str) or not route_id:
            continue
        short_name = getattr(row, "route_short_name", None)
        long_name = getattr(row, "route_long_name", None)
        rows.append(
            {
                "route_id": route_id,
                "route_short_name": (
                    short_name if isinstance(short_name, str) else None
                ),
                "route_long_name": long_name if isinstance(long_name, str) else None,
                "mode": modes.get(route_id, "other"),
                "version_slug": version_slug,
            }
        )
    return rows


def _route_aliases_rows(gtfs, version_slug: str) -> list[dict]:
    rows = []
    for row in gtfs.routes.itertuples(index=False):
        route_id = getattr(row, "route_id", None)
        if not isinstance(route_id, str) or not route_id:
            continue
        for col, alias_type in (
            ("route_short_name", "short_name"),
            ("route_long_name", "long_name"),
        ):
            token = getattr(row, col, None)
            if not isinstance(token, str) or not token.strip():
                continue
            rows.append(
                {
                    "alias_token": token.strip(),
                    "alias_type": alias_type,
                    "route_id": route_id,
                    "version_slug": version_slug,
                }
            )
    return rows


def _shapes_rows(gtfs, version_slug: str) -> list[dict]:
    rows = []
    for row in gtfs.shapes.itertuples(index=False):
        shape_id = getattr(row, "shape_id", None)
        if not isinstance(shape_id, str) or not shape_id:
            continue
        seq = getattr(row, "shape_pt_sequence", None)
        lat = getattr(row, "shape_pt_lat", None)
        lon = getattr(row, "shape_pt_lon", None)
        if pd.isna(seq) or pd.isna(lat) or pd.isna(lon):
            continue
        dist = getattr(row, "shape_dist_traveled", None)
        rows.append(
            {
                "shape_id": shape_id,
                "shape_pt_sequence": int(seq),
                "shape_pt_lat": float(lat),
                "shape_pt_lon": float(lon),
                "shape_dist_traveled": float(dist) if pd.notna(dist) else None,
                "version_slug": version_slug,
            }
        )
    return rows


def _calendar_rows(gtfs, version_slug: str) -> list[dict]:
    rows = []
    for row in gtfs.calendar.itertuples(index=False):
        service_id = getattr(row, "service_id", None)
        if not isinstance(service_id, str) or not service_id:
            continue
        start_date = _iso(getattr(row, "start_date", None))
        end_date = _iso(getattr(row, "end_date", None))
        if start_date is None or end_date is None:
            continue
        weekdays = {}
        ok = True
        for col in _WEEKDAY_COLS:
            val = getattr(row, col, None)
            if val is None or (isinstance(val, float) and pd.isna(val)):
                ok = False
                break
            weekdays[col] = bool(int(val))
        if not ok:
            continue
        rows.append(
            {
                "service_id": service_id,
                **weekdays,
                "start_date": start_date,
                "end_date": end_date,
                "version_slug": version_slug,
            }
        )
    return rows


def _calendar_dates_rows(gtfs, version_slug: str) -> list[dict]:
    rows = []
    for row in gtfs.calendar_dates.itertuples(index=False):
        service_id = getattr(row, "service_id", None)
        if not isinstance(service_id, str) or not service_id:
            continue
        date = _iso(getattr(row, "date", None))
        exception_type = getattr(row, "exception_type", None)
        if date is None or exception_type is None or pd.isna(exception_type):
            continue
        rows.append(
            {
                "service_id": service_id,
                "date": date,
                "exception_type": int(exception_type),
                "version_slug": version_slug,
            }
        )
    return rows


def _route_patterns_rows(gtfs, version_slug: str) -> list[dict]:
    rows = []
    for row in gtfs.route_patterns.itertuples(index=False):
        route_pattern_id = getattr(row, "route_pattern_id", None)
        route_id = getattr(row, "route_id", None)
        if not isinstance(route_pattern_id, str) or not route_pattern_id:
            continue
        if not isinstance(route_id, str) or not route_id:
            continue
        direction_id = getattr(row, "direction_id", None)
        if direction_id is None or pd.isna(direction_id):
            continue
        name = getattr(row, "route_pattern_name", None)
        typicality = getattr(row, "route_pattern_typicality", None)
        rep_trip = getattr(row, "representative_trip_id", None)
        rows.append(
            {
                "route_pattern_id": route_pattern_id,
                "route_id": route_id,
                "direction_id": int(direction_id),
                "route_pattern_name": name if isinstance(name, str) else None,
                "route_pattern_typicality": (
                    int(typicality) if pd.notna(typicality) else None
                ),
                "representative_trip_id": (
                    rep_trip if isinstance(rep_trip, str) else None
                ),
                "version_slug": version_slug,
            }
        )
    return rows


def _directions_rows(gtfs, version_slug: str) -> list[dict]:
    rows = []
    for row in gtfs.directions.itertuples(index=False):
        route_id = getattr(row, "route_id", None)
        if not isinstance(route_id, str) or not route_id:
            continue
        direction_id = getattr(row, "direction_id", None)
        if direction_id is None or pd.isna(direction_id):
            continue
        direction = getattr(row, "direction", None)
        destination = getattr(row, "direction_destination", None)
        rows.append(
            {
                "route_id": route_id,
                "direction_id": int(direction_id),
                "direction": direction if isinstance(direction, str) else None,
                "direction_destination": (
                    destination if isinstance(destination, str) else None
                ),
                "version_slug": version_slug,
            }
        )
    return rows


def _checkpoints_rows(gtfs, version_slug: str) -> list[dict]:
    rows = []
    for row in gtfs.checkpoints.itertuples(index=False):
        checkpoint_id = getattr(row, "checkpoint_id", None)
        if not isinstance(checkpoint_id, str) or not checkpoint_id:
            continue
        name = getattr(row, "checkpoint_name", None)
        rows.append(
            {
                "checkpoint_id": checkpoint_id,
                "checkpoint_name": name if isinstance(name, str) else None,
                "version_slug": version_slug,
            }
        )
    return rows


def _route_shapes_rows(gtfs, version_slug: str) -> list[dict]:
    rows = []
    for (route_id, direction_id), shape_ids in gtfs.route_direction_shapes.items():
        for shape_id in shape_ids:
            points = gtfs.shape_points.get(shape_id)
            if not points or len(points) < 2:
                continue
            cumulative = cumulative_arc_length_m(points)
            for i, (lat, lon) in enumerate(points):
                rows.append(
                    {
                        "route_id": route_id,
                        "direction_id": direction_id,
                        "shape_id": shape_id,
                        "point_sequence": i,
                        "lat": lat,
                        "lon": lon,
                        "dist_m": cumulative[i],
                        "version_slug": version_slug,
                    }
                )
    return rows


def _route_shape_stops_rows(gtfs, version_slug: str) -> list[dict]:
    rows = []
    for route_id, direction_id in gtfs.route_direction_shapes:
        by_shape = gtfs.route_direction_stop_offsets(route_id, direction_id)
        for shape_id, offsets in by_shape.items():
            for stop_id, dist_m in offsets.items():
                rows.append(
                    {
                        "route_id": route_id,
                        "direction_id": direction_id,
                        "shape_id": shape_id,
                        "stop_id": stop_id,
                        "dist_m": dist_m,
                        "version_slug": version_slug,
                    }
                )
    return rows


_ROW_BUILDERS = {
    "gtfs_stops": _stops_rows,
    "gtfs_routes": _routes_rows,
    "gtfs_route_aliases": _route_aliases_rows,
    "gtfs_calendar": _calendar_rows,
    "gtfs_calendar_dates": _calendar_dates_rows,
    "gtfs_shapes": _shapes_rows,
    "gtfs_route_patterns": _route_patterns_rows,
    "gtfs_directions": _directions_rows,
    "gtfs_checkpoints": _checkpoints_rows,
    "route_shapes": _route_shapes_rows,
    "route_shape_stops": _route_shape_stops_rows,
}


def process_feed_day(
    feed: str,
    day: dt.date,
    resolver: GtfsResolver,
    curated_dir: Path,
    force: bool,
    written_versions: set[tuple[str, str]],
    gtfs_scope: dict | None = None,
) -> dict | None:
    """Build this (feed, day)'s manifest row, and the version marts if new.

    `gtfs_scope` scopes a multi-operator static schedule to just this feed's
    own operator (see load_feed_gtfs_scope and StaticGtfs's docstring) -- {}
    for every feed that doesn't need it, which is most of them.

    Returns a counts dict, or None when no snapshot covers this day. Raises
    requests.exceptions.RequestException on catalog/zip fetch failure — the
    caller decides whether that poisons the rest of this agency's days.
    """
    try:
        snap = pick_snapshot(resolver.catalog(), day)
    except LookupError as e:
        print(f"[{feed} {day}] SKIP — {e}", file=sys.stderr)
        return None

    result = {m: 0 for m in _VERSION_MARTS}
    result["manifest"] = 0
    key = (feed, snap.version_slug)
    version_paths = {
        m: _version_partition_path(curated_dir, m, feed, snap.version_slug)
        for m in _VERSION_MARTS
    }
    already_on_disk = all(p.exists() for p in version_paths.values())
    if key not in written_versions and (force or not already_on_disk):
        gtfs_day = resolver.for_date(day, **(gtfs_scope or {}))
        for mart in _VERSION_MARTS:
            mart_rows = _ROW_BUILDERS[mart](gtfs_day, snap.version_slug)
            _write_parquet(mart_rows, _MART_SCHEMAS[mart], version_paths[mart])
            result[mart] = len(mart_rows)
        print(
            f"[{feed} {day}] version {snap.version_slug}: "
            f"{result['gtfs_stops']:>6,} stops  "
            f"{result['gtfs_routes']:>4,} routes  "
            f"{result['gtfs_route_aliases']:>4,} route_aliases  "
            f"{result['gtfs_calendar']:>4,} calendar  "
            f"{result['gtfs_calendar_dates']:>5,} calendar_dates  "
            f"{result['gtfs_shapes']:>7,} shape points  "
            f"{result['gtfs_route_patterns']:>4,} route_patterns  "
            f"{result['gtfs_directions']:>3,} directions  "
            f"{result['gtfs_checkpoints']:>4,} checkpoints  "
            f"{result['route_shapes']:>6,} route_shape points  "
            f"{result['route_shape_stops']:>4,} route_shape_stops"
        )
    written_versions.add(key)

    manifest_path = _mart_path(curated_dir, _VERSIONS_MART, feed, day)
    if force or not manifest_path.exists():
        manifest_row = [
            {
                "service_date": day.isoformat(),
                "version_slug": snap.version_slug,
                "feed_version": snap.feed_version,
                "feed_start_date": snap.feed_start_date.isoformat(),
                "feed_end_date": snap.feed_end_date.isoformat(),
            }
        ]
        _write_parquet(manifest_row, GTFS_VERSIONS_SCHEMA, manifest_path)
        result["manifest"] = 1

    return result


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    feed_agency_map = load_feed_agency_map(args.config)
    feed_gtfs_scope = load_feed_gtfs_scope(args.config)
    feeds = (
        args.feed
        if args.feed
        else sorted(f for f, (_, mdb) in feed_agency_map.items() if mdb)
    )
    base_dates = [args.day] if args.day else []

    # Each GtfsResolver memoizes every StaticGtfs it loads (stops/routes/shapes
    # as pandas DataFrames -- can be tens of MB for a large agency) and never
    # frees it, so keeping one resolver alive per agency for the whole run
    # scales memory with total agency count, not with any single feed's size.
    # remaining_feeds_for_agency tracks how many more times each agency_id
    # will be seen in `feeds`; once it hits 0 that agency's resolver (and
    # every StaticGtfs it's holding) is released instead of kept to the end.
    remaining_feeds_for_agency = Counter(
        info[0]
        for f in feeds
        if (info := feed_agency_map.get(f)) is not None and info[1]
    )

    resolver_cache: dict[str, GtfsResolver] = {}
    failed_catalog: set[str] = set()
    written_versions: set[tuple[str, str]] = set()
    totals = {m: 0 for m in _VERSION_MARTS}
    totals["manifest"] = 0

    for feed in feeds:
        info = feed_agency_map.get(feed)
        if info is None or not info[1]:
            print(
                f"[{feed}] no mdb_feed_id for this agency — skipping", file=sys.stderr
            )
            continue
        agency_id, mdb_id = info

        try:
            if agency_id in failed_catalog:
                continue

            dates = set(base_dates)
            if args.all_days:
                dates |= set(discover_dates(feed, args.curated_dir, args.source))
            if not dates:
                print(f"[{feed}] no dates to process — skipping", file=sys.stderr)
                continue

            if agency_id not in resolver_cache:
                resolver_cache[agency_id] = GtfsResolver(
                    mdb_id,
                    agency_id.lower(),
                    cache_dir=args.gtfs_cache_dir,
                    api_url=args.gtfs_api_url,
                )
            resolver = resolver_cache[agency_id]

            for day in sorted(dates):
                try:
                    result = process_feed_day(
                        feed,
                        day,
                        resolver,
                        args.curated_dir,
                        args.force,
                        written_versions,
                        gtfs_scope=feed_gtfs_scope.get(feed),
                    )
                except requests.exceptions.RequestException as e:
                    print(
                        f"[{feed}] GTFS catalog/zip unavailable ({e}) — "
                        f"skipping agency {agency_id}",
                        file=sys.stderr,
                    )
                    failed_catalog.add(agency_id)
                    break
                if result is not None:
                    for k in totals:
                        totals[k] += result[k]
        finally:
            remaining_feeds_for_agency[agency_id] -= 1
            if remaining_feeds_for_agency[agency_id] <= 0:
                resolver_cache.pop(agency_id, None)

    print(
        f"---\ntotal: {totals['gtfs_stops']:,} stops, "
        f"{totals['gtfs_routes']:,} routes, "
        f"{totals['gtfs_route_aliases']:,} route_aliases, "
        f"{totals['gtfs_calendar']:,} calendar rows, "
        f"{totals['gtfs_calendar_dates']:,} calendar_dates rows, "
        f"{totals['gtfs_shapes']:,} shape points, "
        f"{totals['gtfs_route_patterns']:,} route_patterns rows, "
        f"{totals['gtfs_directions']:,} directions rows, "
        f"{totals['gtfs_checkpoints']:,} checkpoints rows, "
        f"{totals['route_shapes']:,} route_shape points, "
        f"{totals['route_shape_stops']:,} route_shape_stops rows, "
        f"{totals['manifest']:,} version-manifest rows"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
