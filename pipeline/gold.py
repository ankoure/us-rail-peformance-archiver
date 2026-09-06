"""Build the gold tier: daily performance-metrics marts from curated parquet.

For each (feed, day) this reads the silver `vehicles` (or `trip_updates`)
partition, derives Visits, folds them with `analysis.metrics.compute_marts`, and
writes two parquet marts:

    {curated}/metrics/stop_day/feed={feed}/year=/month=/day=/data.parquet
    {curated}/metrics/route_day/feed={feed}/year=/month=/day=/data.parquet

and an event-grain mart with one ARR/DEP row per stop visit, all routes in one
partition per day:

    {curated}/metrics/events/feed={feed}/year=/month=/day=/data.parquet

When the feed's agency declares an `mdb_feed_id`, it also joins each visit to the
static GTFS schedule (`analysis.adherence`) and writes the on-time-performance
marts — a trip-stop fact table plus stop-day / route-day rollups:

    {curated}/metrics/adherence/feed={feed}/year=/month=/day=/data.parquet
    {curated}/metrics/stop_day_otp/...
    {curated}/metrics/route_day_otp/...

It also writes a small routes-manifest mart (route_id -> short/long name,
mode) off the same resolved GTFS snapshot, so dashboard/api can read a
route list without resolving GTFS itself:

    {curated}/metrics/routes/feed={feed}/year=/month=/day=/data.parquet

OTP runs in the prod daily batch (pandas is a runtime dependency). It is still
best-effort: it self-skips when an agency has no mdb_feed_id, when the schedule
snapshot can't be resolved, or — defensively — when pandas is unavailable, and
none of these block the schedule-free marts. Pass --no-otp to skip it explicitly.

It mirrors rollup.py's CLI shell and ship.py's per-(feed, day) loop, so the prod
batch chain can call `python pipeline/gold.py --day "$DAY"` symmetrically between
rollup and ship. Missing partitions skip rather than abort.

Examples:

    # one feed, one day (schedule-free marts + OTP, GTFS auto-resolved)
    uv run python pipeline/gold.py --feed wmata-vehicles --day 2026-05-20

    # every feed in the config, every day already on disk (auto-picks
    # trip-updates for _TRIP_UPDATES_ONLY_FEEDS, vehicles for everything else)
    uv run python pipeline/gold.py --all-days

    # force trip-updates for a feed not in _TRIP_UPDATES_ONLY_FEEDS yet
    uv run python pipeline/gold.py --feed metromn-trips --source trip-updates --day 2026-05-22

    # schedule-free marts only, with a stricter on-time window
    uv run python pipeline/gold.py --all-days --no-otp
    uv run python pipeline/gold.py --feed metra-vehicles --day 2026-05-20 --late-threshold-seconds 360
"""

from __future__ import annotations

import argparse
import datetime as dt
import re
import sys
from pathlib import Path
from zoneinfo import ZoneInfo

import pyarrow as pa
import pyarrow.parquet as pq
from dotenv import load_dotenv

# Make the repo root importable when run as `python pipeline/gold.py`.
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from analysis.metrics import (  # noqa: E402
    EVENTS_SCHEMA,
    ROUTE_DAY_SCHEMA,
    STOP_DAY_SCHEMA,
    compute_events,
    compute_marts,
)
from analysis.curated_fs import curated_fs, has_any, list_parquet  # noqa: E402
from analysis.trip_updates_day import TripUpdatesDay  # noqa: E402
from analysis.vehicle_day import VehicleDay  # noqa: E402
from archiver.loader import load_config  # noqa: E402
from pipeline.agency_map import load_feed_agency_map  # noqa: E402

load_dotenv()

_PART_RE = re.compile(r"^(year|month|day)=(\d+)$")
_SOURCE_SUBDIR = {"vehicles": "vehicles", "trip-updates": "trip_updates"}

# Feeds built from TripUpdates rather than VehiclePositions: `main()` forces
# these onto --source trip-updates by default (see `parse_args`'s --source
# default=None) so the daily automated batch (which never passes --source)
# picks it up without a per-feed CLI invocation. Every entry has a TripUpdates
# feed with 100%-populated stop_id. Two shapes qualify:
#
#   - **VehiclePositions can't yield a dwell at all**: they never carry
#     stop_id/current_stop_sequence, so there's nothing to group a run of pings
#     by — or there's no vehicles feed in the first place. Without this list
#     they'd silently produce zero gold marts forever. Confirmed by auditing
#     every onboarded feed on 2026-08-13 and again on 2026-08-31: each is
#     paired with either no vehicles feed, or one that resolves GTFS fine
#     (writes the `routes` mart) but yields zero events/adherence rows daily.
#
#   - **TripUpdates are simply better**: the VehiclePositions would now produce
#     something — see the never-STOPPED_AT class handled by
#     `VehicleDay.publishes_stopped_at` — but only arrival-only, dwell-0
#     estimates, while the agency's TripUpdates carry absolute arrival *and*
#     departure times and so give true dwells. Listing these is a quality
#     choice, not a rescue; a feed in this shape still produces marts if left
#     off the list.
#
# 2026-08-16: added the 7 trips-only agencies from the Aug 14 global feed
# merge (config/feeds.candidates.validated.yaml) — none have a vehicles feed
# at all, so without this they'd silently produce zero gold marts forever.
#
# 2026-08-31: audited the 12 agencies that resolve GTFS but produce no events
# and no OTP. Added Adelaide Metro, Connecticut Transit and MiWay (vehicles
# carry no stop_id), MTA_524 (no vehicles feed at all), and Sofia — the first
# confirmed never-STOPPED_AT feed (5 live snapshots, ~2,900 pings, zero
# STOPPED_AT), which lands in the second shape above: its TripUpdates carry
# stop_id, arrival.time and departure.time on 100% of stop_time_updates, and
# every RT trip_id/stop_id matches the mdb-2848 static snapshot.
#
# Vilnius and Fuenlabrada are the same never-STOPPED_AT class but are NOT
# listed: their TripUpdates publish `delay` only, with no absolute times, so
# TripUpdatesDay has nothing to build a Visit from. They rely on the
# position-based fallback instead.
_TRIP_UPDATES_ONLY_FEEDS = {
    "adelaide-metro-trips-1773",
    "arlington-transit-trips-1372",
    "bart-trips",
    "beach-cities-transit-trips-1564",
    "big-blue-bus-trips-1438",
    "capital-area-transportation-authority-trips-1426",
    "c-tran-trips-1922",
    "cincinnati-metro-trips-1488",
    "commerce-municipal-bus-lines-trips-1795",
    "connecticut-transit-trips-2423",
    "dpn-avl-cfl-cflbus-rgtr-tice-tram-trips-1640",
    "el-paso-transportation-authority-trips-3203",
    "emery-go-round-trips-2226",
    "everett-transit-trips-3073",
    "gtfs-de-delfi-realtime-data-trips-alerts-3101",
    "il-via-m-tropole-europ-enne-de-lille-trips-1792",
    "maryland-transit-administration-vehicles-trips-1619",
    "metra-trips",
    "metro-houston-trips",
    "metropolitan-transit-authority-mta-trips-1627",
    "metropolitan-transit-authority-mta-vehicles-trips-1624",
    "metrostl-trips",
    "metro-transit-city-of-madison-trips-2096",
    "milwaukee-county-transit-system-mcts-trips-2128",
    "miway-trips-1611",
    "mountain-line-transit-trips-1981",
    "mountain-view-transportation-management-association-mvgo-trips-2109",
    "mts-trips",
    "nashville-mta-wego-public-transit-trips-1620",
    "nyc-ferry-trips-1638",
    "ny-waterway-trips-3198",
    "ny-waterway-trips-3200",
    "orange-county-transportation-authority-octa-trips-1641",
    "pasadena-transit-trips-1655",
    "polskie-koleje-pa-stwowe-intercity-pkp-intercity-trips-2924",
    "prt-trips",
    "rea-metropolitana-de-barcelona-amb-trips-2368",
    "riverside-transit-agency-trips-1663",
    "sacrt-trips",
    "santa-cruz-metro-scmtd-trips-1911",
    "south-metro-area-regional-transit-smart-trips-2697",
    "springfield-mass-transit-district-smtd-trips-1780",
    "st-trips",
    "trirail-trips",
    "trolleybus-transport-pleven-trips-2120",
    "urban-mobility-center-sofia-traffic-trips-2850",
    "uta-trips",
    "valley-regional-transit-trips-2453",
    "verkehrsverbund-berlin-brandenburg-vbb-trips-2378",
}


def superseded_by_trip_updates(config_path: Path) -> set[str]:
    """The other feeds of every agency built from TripUpdates.

    When an agency is on `_TRIP_UPDATES_ONLY_FEEDS` its marts come from the
    TripUpdates feed; running its vehicles feed as well would write a second,
    lower-quality set of rows for the same (agency, day) under a different
    `feed=` partition. dashboard/api reads every feed an agency owns, so those
    rows are not a harmless duplicate — they double-count.

    This never mattered while the list only held feeds whose VehiclePositions
    can't yield a dwell at all: their vehicles feed wrote nothing, so the
    one-source-per-agency invariant held by accident. The never-STOPPED_AT
    class breaks that, because the position-based fallback now *does* produce
    visits for them (Sofia, Valley Regional Transit).

    Only the automated all-feeds enumeration is filtered; an explicit
    `--feed`/`--source` still builds exactly what the operator asked for.
    """
    config = load_config(str(config_path))
    superseded: set[str] = set()
    for agency in config.agencies:
        names = [f.name for f in agency.feeds]
        if not any(n in _TRIP_UPDATES_ONLY_FEEDS for n in names):
            continue
        superseded |= {n for n in names if n not in _TRIP_UPDATES_ONLY_FEEDS}
    return superseded


_SCHEDULE_FREE_MARTS = ("stop_day", "route_day", "events")
_OTP_MARTS = ("adherence", "stop_day_otp", "route_day_otp")
_SPEED_MARTS = ("segment_speed", "segment_day")
_ROUTES_MART = "routes"

# One row per GTFS route_id — a small, rarely-changing reference table, rebuilt
# daily alongside OTP (same gtfs_for resolver, no second download) so
# dashboard/api can read it via the same S3 hot-bucket + read_kind path as
# every other mart, instead of resolving GTFS itself at request time.
ROUTES_SCHEMA = pa.schema(
    [
        pa.field("route_id", pa.string(), nullable=False),
        pa.field("route_short_name", pa.string(), nullable=True),
        pa.field("route_long_name", pa.string(), nullable=True),
        pa.field("route_color", pa.string(), nullable=True),
        pa.field("mode", pa.string(), nullable=False),
        pa.field("service_date", pa.string(), nullable=False),
    ]
)

# GTFS resolver defaults — duplicated from analysis.gtfs_fetcher rather than
# imported, matching that module's own duplication in pipeline/gtfs.py. (Stale
# note removed 2026-09-06: pandas is a main dependency, not dev-only, and this
# path runs in prod every night via stage-gold/heavy-gold's OTP marts -- the
# "keeps gold.py pandas-free" reasoning this comment used to give was never
# actually true of the deployed image, only of an import-time lazy-load
# inside main(), which still applies and is the real reason to duplicate
# rather than import at module scope.) Keep in sync with
# analysis/gtfs_fetcher.py's DEFAULT_API_URL -- MDB_REFRESH_TOKEN must be set
# in the environment for this to authenticate.
_DEFAULT_GTFS_API_URL = "https://api.mobilitydatabase.org/v1"
_DEFAULT_GTFS_CACHE_DIR = Path("data/static_gtfs")


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    p.add_argument(
        "--feed",
        nargs="+",
        default=None,
        help="One or more feed names. Omit to process every feed in the config.",
    )
    p.add_argument(
        "--day",
        type=dt.date.fromisoformat,
        help="A single day (YYYY-MM-DD) to build.",
    )
    p.add_argument(
        "--all-days",
        action="store_true",
        help="Build every service_date partition on disk for each feed "
        "(unioned with --day).",
    )
    p.add_argument(
        "--source",
        choices=["vehicles", "trip-updates"],
        default=None,
        help="Which curated dataset to derive Visits from. Omit to auto-pick per "
        "feed: trip-updates for _TRIP_UPDATES_ONLY_FEEDS, vehicles otherwise. "
        "Passing this explicitly forces every requested feed onto that source.",
    )
    p.add_argument(
        "--curated-dir",
        type=Path,
        default=Path("data/curated"),
        help="Curated root: read silver from here and write metrics under it. "
        "Must stay a LOCAL path -- marts are written here.",
    )
    p.add_argument(
        "--silver-dir",
        default=None,
        help="Read silver (vehicles/trip_updates/swiv_ligne) from here instead "
        "of --curated-dir. Accepts an s3:// URI, e.g. "
        "s3://rail-performance-archiver-hot -- the hot bucket IS the curated "
        "tree (Shipper._hot_key is the curated-relative path), so this lets "
        "gold run in its own ECS task off rollup's shipped output rather than "
        "needing shared local disk with it. Marts still go to --curated-dir.",
    )
    p.add_argument(
        "-c",
        "--config",
        type=Path,
        default=Path("config/feeds.yaml"),
        help="Path to feeds.yaml (feed->timezone lookup and default feed list).",
    )
    p.add_argument(
        "--merge-gap-seconds",
        type=int,
        default=60,
        help="Collapse same-stop visits separated by <= this many seconds "
        "(default: 60). Set to 0 to disable.",
    )
    p.add_argument(
        "--no-otp",
        action="store_true",
        help="Skip the on-time-performance (schedule-adherence) marts; build only "
        "the schedule-free marts. OTP also self-skips when a feed's agency has no "
        "mdb_feed_id or the schedule can't be resolved.",
    )
    p.add_argument(
        "--early-threshold-seconds",
        type=int,
        default=60,
        help="A visit arriving more than this many seconds early is 'early' "
        "(default: 60).",
    )
    p.add_argument(
        "--late-threshold-seconds",
        type=int,
        default=300,
        help="A visit arriving more than this many seconds late is 'late' "
        "(default: 300). Between the two thresholds is 'on_time'.",
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
        help="MobilityDatabase API base URL for resolving GTFS snapshots "
        "(requires MDB_REFRESH_TOKEN in the environment).",
    )
    p.add_argument(
        "-f", "--force", action="store_true", help="Overwrite existing marts."
    )
    p.add_argument(
        "--normalize",
        action="store_true",
        help="Instead of building metrics marts, normalize the JSON-feed curated "
        "tables into curated/normalized_vehicles (resolve raw ids -> GTFS ids, "
        "standardize units/time).",
    )
    args = p.parse_args(argv)
    if not args.day and not args.all_days:
        p.error("provide --day YYYY-MM-DD or --all-days")
    return args


def load_feed_tz_map(config_path: Path) -> dict[str, str]:
    """feed_name -> IANA timezone, gathered from every agency in the config."""
    config = load_config(str(config_path))
    return {
        feed.name: agency.timezone
        for agency in config.agencies
        for feed in agency.feeds
    }


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
    """Every service_date with a partition for (feed, source) under curated_dir."""
    return discover_dates_for_dir(feed, curated_dir / _SOURCE_SUBDIR[source])


def _partition_path(root: Path, feed: str, day: dt.date) -> Path:
    """`<root>/feed={feed}/year=/month=/day=/data.parquet` — the curated layout.

    The single source of truth for the partition path shape; each caller supplies
    its own `<root>` (table dir, metrics mart, normalized_vehicles, ...).
    """
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


def _write_parquet(rows: list[dict], schema: pa.Schema, path: Path) -> None:
    """Atomic write via tmp + rename (mirrors Rollup._write_parquet)."""
    table = pa.Table.from_pylist(rows, schema=schema)
    tmp = path.with_suffix(".parquet.tmp")
    tmp.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(table, tmp)
    tmp.rename(path)


def build_one(
    feed: str,
    day: dt.date,
    tz: ZoneInfo,
    curated_dir: Path,
    source: str,
    merge_gap_seconds: int,
    force: bool,
    *,
    silver_dir: Path | str | None = None,
    gtfs_for=None,
    early_threshold_s: int = 60,
    late_threshold_s: int = 300,
) -> dict | None:
    """Build the marts for one (feed, day). Returns a counts dict, or None.

    `curated_dir` is where marts are WRITTEN and must stay a local path.
    `silver_dir` is where silver is READ from and may be an s3:// URI (defaults
    to curated_dir). Keeping them separate is what lets gold run in its own ECS
    task, reading rollup's output from the hot bucket while still writing its
    own marts to local disk for ship.py to upload -- and it keeps an s3:// value
    from ever reaching the write paths, which would land marts in a local
    directory literally named "s3:".

    Three independent, separately-idempotent steps off a single Visit load: the
    schedule-free marts (stop_day / route_day / events), the on-time-performance
    marts (adherence / stop_day_otp / route_day_otp), and the segment-speed marts
    (segment_speed / segment_day). Both GTFS-gated steps share the same resolver.
    Returns None when there's nothing to do (all marts already built without
    --force, or no partition on disk).

    `gtfs_for(feed, day) -> StaticGtfs | None` resolves the schedule snapshot; it
    is None when OTP is disabled. GTFS lookup failures skip only the OTP/speed steps.
    """
    sf_paths = {m: _mart_path(curated_dir, m, feed, day) for m in _SCHEDULE_FREE_MARTS}
    otp_paths = {m: _mart_path(curated_dir, m, feed, day) for m in _OTP_MARTS}
    speed_paths = {m: _mart_path(curated_dir, m, feed, day) for m in _SPEED_MARTS}
    routes_path = _mart_path(curated_dir, _ROUTES_MART, feed, day)
    need_sf = force or not all(p.exists() for p in sf_paths.values())
    need_otp = gtfs_for is not None and (
        force or not all(p.exists() for p in otp_paths.values())
    )
    need_speed = gtfs_for is not None and (
        force or not all(p.exists() for p in speed_paths.values())
    )
    need_routes = gtfs_for is not None and (force or not routes_path.exists())
    if not need_sf and not need_otp and not need_speed and not need_routes:
        print(f"[{feed} {day}] exists — skipping (use --force)", file=sys.stderr)
        return None

    day_cls = VehicleDay if source == "vehicles" else TripUpdatesDay
    try:
        vd = day_cls(
            feed,
            day,
            base_dir=silver_dir if silver_dir is not None else curated_dir,
            merge_gap_seconds=merge_gap_seconds,
        )
        visits = [v for veh in vd.vehicles for v in veh.dwells]
    except FileNotFoundError as e:
        print(f"[{feed} {day}] SKIP — {e}", file=sys.stderr)
        return None

    result = {
        "stop": 0,
        "route": 0,
        "events": 0,
        "adherence": 0,
        "on_time": 0,
        "segments": 0,
        "routes": 0,
    }
    if need_sf:
        stop_rows, route_rows = compute_marts(visits, feed, tz)
        event_rows = compute_events(visits, feed, tz)
        if stop_rows or route_rows or event_rows:
            _write_parquet(stop_rows, STOP_DAY_SCHEMA, sf_paths["stop_day"])
            _write_parquet(route_rows, ROUTE_DAY_SCHEMA, sf_paths["route_day"])
            _write_parquet(event_rows, EVENTS_SCHEMA, sf_paths["events"])
            result.update(
                stop=len(stop_rows), route=len(route_rows), events=len(event_rows)
            )
            print(
                f"[{feed} {day}] {len(stop_rows):>7,} stop-day  "
                f"{len(route_rows):>6,} route-day  {len(event_rows):>8,} events"
            )
        else:
            print(f"[{feed} {day}] no metrics rows", file=sys.stderr)

    if need_otp:
        otp = _build_otp(
            feed,
            day,
            tz,
            visits,
            gtfs_for,
            otp_paths,
            early_threshold_s,
            late_threshold_s,
        )
        if otp is not None:
            result.update(adherence=otp[0], on_time=otp[1])

    if need_speed:
        n = _build_speed(feed, day, tz, visits, gtfs_for, speed_paths)
        if n is not None:
            result["segments"] = n

    if need_routes:
        n = _build_routes(feed, day, gtfs_for, routes_path)
        if n is not None:
            result["routes"] = n

    return result


def _build_otp(
    feed: str,
    day: dt.date,
    tz: ZoneInfo,
    visits: list,
    gtfs_for,
    otp_paths: dict[str, Path],
    early_threshold_s: int,
    late_threshold_s: int,
) -> tuple[int, int] | None:
    """Build the OTP marts for one (feed, day). Returns (matched, on_time) or None.

    Imports the pandas-backed GTFS path lazily so gold.py stays importable (and
    the schedule-free marts keep building) where pandas is absent. Resolves the
    schedule for `day` plus the prior day (owl trips), folds adherence, and writes
    the three OTP marts. Any GTFS lookup failure skips OTP for this (feed, day).
    """
    from analysis.adherence import (
        ADHERENCE_SCHEMA,
        ROUTE_DAY_OTP_SCHEMA,
        STOP_DAY_OTP_SCHEMA,
        compute_adherence,
        schedule_index,
    )

    try:
        gtfs_day = gtfs_for(feed, day)
    except (LookupError, FileNotFoundError) as e:
        print(f"[{feed} {day}] SKIP OTP — GTFS lookup: {e}", file=sys.stderr)
        return None
    if gtfs_day is None:
        print(
            f"[{feed} {day}] no mdb_feed_id for this agency — skipping OTP",
            file=sys.stderr,
        )
        return None

    schedules = {day: schedule_index(gtfs_day, day)}
    prev = day - dt.timedelta(days=1)
    try:  # prior-day schedule backs owl trips; best-effort.
        gtfs_prev = gtfs_for(feed, prev)
        if gtfs_prev is not None:
            schedules[prev] = schedule_index(gtfs_prev, prev)
    except (LookupError, FileNotFoundError):
        pass

    fact, stop_rows, route_rows = compute_adherence(
        visits,
        schedules,
        feed,
        tz,
        early_threshold_s=early_threshold_s,
        late_threshold_s=late_threshold_s,
        route_modes=gtfs_day.route_modes,
    )
    if not fact:
        print(f"[{feed} {day}] no schedule matches — skipping OTP", file=sys.stderr)
        return None

    _write_parquet(fact, ADHERENCE_SCHEMA, otp_paths["adherence"])
    _write_parquet(stop_rows, STOP_DAY_OTP_SCHEMA, otp_paths["stop_day_otp"])
    _write_parquet(route_rows, ROUTE_DAY_OTP_SCHEMA, otp_paths["route_day_otp"])

    matched = len(fact)
    on_time = sum(1 for r in fact if r["on_time"])
    candidates = sum(1 for v in visits if v.trip_id)
    pct = 100 * on_time / matched if matched else 0.0
    rate = 100 * matched / candidates if candidates else 0.0
    print(
        f"[{feed} {day}] {matched:>8,} adherence  {pct:5.1f}% on-time  "
        f"({rate:4.1f}% of {candidates:,} trip-visits matched schedule)"
    )
    return matched, on_time


def _build_routes(
    feed: str,
    day: dt.date,
    gtfs_for,
    routes_path: Path,
) -> int | None:
    """Build the routes-manifest mart for one (feed, day). Returns row count, or None.

    Shares gtfs_for(feed, day) with _build_otp — GtfsResolver memoizes snapshot
    loads per agency, so when OTP already resolved this (feed, day) this call is
    a cache hit, not a second download.
    """
    try:
        gtfs_day = gtfs_for(feed, day)
    except (LookupError, FileNotFoundError) as e:
        print(f"[{feed} {day}] SKIP routes — GTFS lookup: {e}", file=sys.stderr)
        return None
    if gtfs_day is None:
        print(
            f"[{feed} {day}] no mdb_feed_id for this agency — skipping routes",
            file=sys.stderr,
        )
        return None

    route_modes = gtfs_day.route_modes
    rows = []
    for row in gtfs_day.routes.itertuples(index=False):
        route_id = getattr(row, "route_id", None)
        if not isinstance(route_id, str) or not route_id:
            continue
        short_name = getattr(row, "route_short_name", None)
        long_name = getattr(row, "route_long_name", None)
        color = getattr(row, "route_color", None)
        rows.append(
            {
                "route_id": route_id,
                "route_short_name": short_name if isinstance(short_name, str) else None,
                "route_long_name": long_name if isinstance(long_name, str) else None,
                "route_color": color if isinstance(color, str) and color else None,
                "mode": route_modes.get(route_id, "other"),
                "service_date": day.isoformat(),
            }
        )
    if not rows:
        print(f"[{feed} {day}] no routes in schedule — skipping", file=sys.stderr)
        return None

    _write_parquet(rows, ROUTES_SCHEMA, routes_path)
    print(f"[{feed} {day}] {len(rows):>7,} routes")
    return len(rows)


def _build_speed(
    feed: str,
    day: dt.date,
    tz: ZoneInfo,
    visits: list,
    gtfs_for,
    speed_paths: dict[str, Path],
) -> int | None:
    """Build the segment-speed marts for one (feed, day). Returns fact row count or None.

    Resolves the same GTFS snapshot as OTP (so no extra network I/O when both run
    together) and passes stop coordinates plus trip/shape geometry to
    compute_segment_speeds, which prefers shape-following distance and falls
    back to straight-line per trip (see analysis/segment_speed.py). Skips
    silently when the feed has no mdb_feed_id, the GTFS lookup fails, or the
    zip carries no stop coordinates.

    Runs compute_segment_speeds once per mode bucket rather than once for the
    whole feed, so bus routes get BUS_MAX_SPEED_MPH's tighter GPS-artifact
    ceiling instead of the rail-tuned DEFAULT_MAX_SPEED_MPH (see
    analysis/segment_speed.py). Mode comes from gtfs_day.route_modes; a route
    missing from it (e.g. absent from routes.txt) defaults to the rail
    ceiling rather than the bus one, since an unknown mode is not known to be
    a bus.
    """
    from analysis.segment_speed import (
        BUS_MAX_SPEED_MPH,
        DEFAULT_MAX_SPEED_MPH,
        SEGMENT_DAY_SCHEMA,
        SEGMENT_SPEED_SCHEMA,
        compute_segment_speeds,
    )

    try:
        gtfs_day = gtfs_for(feed, day)
    except (LookupError, FileNotFoundError) as e:
        print(f"[{feed} {day}] SKIP speed — GTFS lookup: {e}", file=sys.stderr)
        return None
    if gtfs_day is None:
        return None

    stop_coords = gtfs_day.stop_coords
    if not stop_coords:
        print(
            f"[{feed} {day}] SKIP speed — no stop coordinates in GTFS", file=sys.stderr
        )
        return None

    # Scoped to routes actually observed today -- route_direction_stop_offsets
    # does an O(shape points) projection per (route, direction), so there's no
    # reason to pay for routes with zero visits. See compute_segment_speeds's
    # docstring for why this takes priority over any reported direction_id.
    routes_seen = {v.route_id for v in visits if v.route_id}
    route_direction_offsets: dict[str, dict[int, dict[str, dict[str, float]]]] = {}
    for (route_id, direction_id), _shape_ids in gtfs_day.route_direction_shapes.items():
        if route_id not in routes_seen:
            continue
        offsets = gtfs_day.route_direction_stop_offsets(route_id, direction_id)
        if offsets:
            route_direction_offsets.setdefault(route_id, {})[direction_id] = offsets

    route_modes = gtfs_day.route_modes
    bus_visits = [v for v in visits if route_modes.get(v.route_id) == "bus"]
    other_visits = [v for v in visits if route_modes.get(v.route_id) != "bus"]

    fact_rows: list[dict] = []
    seg_day_rows: list[dict] = []
    for group_visits, max_speed_mph in (
        (other_visits, DEFAULT_MAX_SPEED_MPH),
        (bus_visits, BUS_MAX_SPEED_MPH),
    ):
        if not group_visits:
            continue
        fr, sdr = compute_segment_speeds(
            group_visits,
            stop_coords,
            feed,
            tz,
            shape_by_trip=gtfs_day.trip_shapes,
            shape_points=gtfs_day.shape_points,
            direction_by_trip=gtfs_day.direction_by_trip,
            route_direction_offsets=route_direction_offsets,
            max_speed_mph=max_speed_mph,
        )
        fact_rows.extend(fr)
        seg_day_rows.extend(sdr)
    if not fact_rows:
        print(f"[{feed} {day}] no segment speed rows", file=sys.stderr)
        return None

    _write_parquet(fact_rows, SEGMENT_SPEED_SCHEMA, speed_paths["segment_speed"])
    _write_parquet(seg_day_rows, SEGMENT_DAY_SCHEMA, speed_paths["segment_day"])
    print(
        f"[{feed} {day}] {len(fact_rows):>8,} segment-speed  "
        f"{len(seg_day_rows):>7,} segment-day"
    )
    return len(fact_rows)


def _make_gtfs_resolver(args: argparse.Namespace):
    """Build a `gtfs_for(feed, day) -> StaticGtfs | None` resolver, or None.

    Returns None — with a one-line notice — when pandas isn't installed, so the
    prod batch (which ships without pandas) builds the schedule-free marts and
    silently skips OTP. Otherwise yields a per-agency-cached resolver keyed off
    each agency's mdb_feed_id, mirroring scripts/export_events.py.
    """
    try:
        import requests

        from analysis.gtfs_fetcher import GtfsResolver
    except ImportError as e:
        print(
            f"[otp] disabled — {e}. Install dev deps (pandas) to build OTP marts.",
            file=sys.stderr,
        )
        return None

    feed_agency_map = load_feed_agency_map(args.config)
    cache: dict[str, GtfsResolver] = {}
    # Agencies whose archived-feeds catalog couldn't be fetched (e.g. the
    # mdb_feed_id 404s). Cached so we skip OTP once per agency rather than
    # re-hitting the network for every one of that feed's days.
    failed_catalog: set[str] = set()

    def gtfs_for(feed: str, day: dt.date):
        info = feed_agency_map.get(feed)
        if info is None or not info[1]:
            return None
        agency_id, mdb_id = info
        if agency_id in failed_catalog:
            return None
        if agency_id not in cache:
            cache[agency_id] = GtfsResolver(
                mdb_id,
                agency_id.lower(),
                cache_dir=args.gtfs_cache_dir,
                api_url=args.gtfs_api_url,
            )
        try:
            return cache[agency_id].for_date(day)
        except requests.exceptions.RequestException as e:
            # Catalog/zip fetch failed for the whole agency — skip OTP for it.
            # (Per-date "no snapshot covers this day" raises LookupError, which
            # the caller handles separately; don't poison the agency for that.)
            print(
                f"[{feed}] GTFS catalog unavailable ({e}) — "
                f"skipping OTP for agency {agency_id}",
                file=sys.stderr,
            )
            failed_catalog.add(agency_id)
            return None

    return gtfs_for


def _normalized_path(curated_dir: Path, feed: str, day: dt.date) -> Path:
    return _partition_path(curated_dir / "normalized_vehicles", feed, day)


def _raw_table_path(curated_dir: Path, table: str, feed: str, day: dt.date) -> Path:
    return _partition_path(curated_dir / table, feed, day)


def load_feed_table_map(config_path: Path) -> dict[str, str]:
    """feed_name -> its single curated table, for feeds that have a Normalizer.

    The table is derived from the feed's decoder (`Decoder.produces`), so there's
    one source of truth. Feeds whose decoder emits multiple tables (the GTFS-RT
    `standard` decoder) or whose table has no registered normalizer are omitted.
    """
    from archiver.decoder import Decoder
    from analysis.normalize import Normalizer

    config = load_config(str(config_path))
    out: dict[str, str] = {}
    for agency in config.agencies:
        for feed in agency.feeds:
            specs = list(Decoder.from_name(feed.decoder).produces.values())
            if len(specs) != 1:
                continue
            table = specs[0].name
            if table in Normalizer._registry:
                out[feed.name] = table
    return out


_SWIV_LIGNE_TABLE = "swiv_ligne"


def load_swiv_topo_feed_map(config_path: Path) -> dict[str, str]:
    """agency_id -> the feed name whose decoder lands the swiv_ligne topo table.

    The Swiv route-name sidecar is a separate feed from the vehicles feed.
    Resolving its name from config (rather than assuming `{agency}-topo`) means
    topo resolution starts working the moment that feed is onboarded, under
    whatever name it's given. Empty until that feed exists.
    """
    from archiver.decoder import Decoder

    config = load_config(str(config_path))
    out: dict[str, str] = {}
    for agency in config.agencies:
        for feed in agency.feeds:
            specs = Decoder.from_name(feed.decoder).produces.values()
            if any(s.name == _SWIV_LIGNE_TABLE for s in specs):
                out[agency.agency_id] = feed.name
    return out


def _make_topo_resolver(silver_dir: Path | str, config_path: Path):
    """Build `topo_for(agency_id, day) -> {idLigne: nomCommercial} | None`.

    Reads the landed Swiv topo table (curated/swiv_ligne/feed=<topo feed>/...),
    with the topo feed name resolved from config. Best-effort: returns None when
    no topo feed is configured yet, or its partition isn't there — so Swiv
    routes degrade to unresolved, but with a one-time warning per agency rather
    than silently.

    swiv_ligne is silver (rollup writes it), so this reads from `silver_dir` and
    works over s3:// like the other silver readers.
    """
    fs, base = curated_fs(silver_dir)
    topo_feeds = load_swiv_topo_feed_map(config_path)
    cache: dict[tuple[str, dt.date], dict[str, str] | None] = {}
    warned: set[str] = set()

    def topo_for(agency_id: str, day: dt.date):
        key = (agency_id, day)
        if key in cache:
            return cache[key]
        topo_feed = topo_feeds.get(agency_id)
        result: dict[str, str] | None = None
        if topo_feed is not None:
            prefix = (
                f"{base}/{_SWIV_LIGNE_TABLE}/feed={topo_feed}"
                f"/year={day.year}/month={day.month}/day={day.day}"
            )
            files = list_parquet(fs, prefix)
            if files:
                tbl = pa.concat_tables(
                    [pq.read_table(f, filesystem=fs) for f in files]
                ).to_pylist()
                result = {
                    str(r["id_ligne"]): r["nom_commercial"]
                    for r in tbl
                    if r.get("id_ligne") is not None and r.get("nom_commercial")
                }
        if result is None and agency_id not in warned:
            warned.add(agency_id)
            print(
                f"[{agency_id}] Swiv topo table unavailable "
                f"(feed: {topo_feed or 'none configured'}) — idLigne routes "
                f"resolve by static map only",
                file=sys.stderr,
            )
        cache[key] = result
        return result

    return topo_for


def normalize_one(
    feed: str,
    day: dt.date,
    agency_id: str,
    tz: ZoneInfo,
    table: str,
    curated_dir: Path,
    *,
    gtfs_for,
    topo_for,
    force: bool,
) -> int | None:
    """Normalize one (feed, day) raw table into curated/normalized_vehicles.

    Best-effort, mirroring _build_otp: missing partition -> skip; no resolvable
    static GTFS -> skip. Returns the row count written, or None when skipped.
    """
    from analysis.normalize import NORMALIZED_VEHICLES_SCHEMA, Normalizer

    out = _normalized_path(curated_dir, feed, day)
    if out.exists() and not force:
        print(
            f"[{feed} {day}] normalized exists — skipping (use --force)",
            file=sys.stderr,
        )
        return None

    src = _raw_table_path(curated_dir, table, feed, day)
    if not src.exists():
        print(f"[{feed} {day}] SKIP normalize — no {table} partition", file=sys.stderr)
        return None

    try:
        gtfs = gtfs_for(feed, day)
    except (LookupError, FileNotFoundError) as e:
        print(f"[{feed} {day}] SKIP normalize — GTFS lookup: {e}", file=sys.stderr)
        return None
    if gtfs is None:
        print(
            f"[{feed} {day}] no static GTFS source — skipping normalize",
            file=sys.stderr,
        )
        return None

    normalizer = Normalizer.from_table(table)
    topo = topo_for(agency_id, day) if normalizer.needs_topo else None

    rows = pq.read_table(src).to_pylist()
    normalized = normalizer.normalize(
        rows, gtfs, feed=feed, agency_id=agency_id, agency_tz=tz, topo_map=topo
    )
    _write_parquet(normalized, NORMALIZED_VEHICLES_SCHEMA, out)

    # Count rows that landed at least one canonical GTFS id. Covers route feeds
    # (route_id) and LIRR (trip_id/stop_id), which carries no route token and so
    # would always read 0 against resolution_status alone.
    resolved = sum(
        1
        for r in normalized
        if r["route_id"] is not None
        or r["trip_id"] is not None
        or r["stop_id"] is not None
    )
    print(
        f"[{feed} {day}] {len(normalized):>7,} normalized  ({resolved:,} id-resolved)"
    )
    return len(normalized)


def _run_normalize(args: argparse.Namespace) -> int:
    """The --normalize path: build curated/normalized_vehicles for the JSON feeds."""
    feed_tz_map = load_feed_tz_map(args.config)
    feed_table_map = load_feed_table_map(args.config)
    feed_agency_map = load_feed_agency_map(args.config)
    gtfs_for = _make_gtfs_resolver(args)
    if gtfs_for is None:
        print(
            "[normalize] no GTFS resolver (pandas missing) — nothing to do",
            file=sys.stderr,
        )
        return 0
    topo_for = _make_topo_resolver(args.curated_dir, args.config)

    feeds = args.feed if args.feed else sorted(feed_table_map)
    base_dates = [args.day] if args.day else []
    total = 0
    for feed in feeds:
        table = feed_table_map.get(feed)
        if table is None:
            print(f"[{feed}] no normalizer — skipping", file=sys.stderr)
            continue
        agency_id = feed_agency_map[feed][0]
        tz = ZoneInfo(feed_tz_map[feed])
        dates = set(base_dates)
        if args.all_days:
            dates |= set(discover_dates_for_dir(feed, args.curated_dir / table))
        if not dates:
            print(f"[{feed}] no dates to process — skipping", file=sys.stderr)
            continue
        for day in sorted(dates):
            n = normalize_one(
                feed,
                day,
                agency_id,
                tz,
                table,
                args.curated_dir,
                gtfs_for=gtfs_for,
                topo_for=topo_for,
                force=args.force,
            )
            if n is not None:
                total += n
    print(f"---\ntotal: {total:,} normalized vehicle rows")
    return 0


def assert_silver_present(silver_dir: Path | str) -> None:
    """Fail loudly when the silver tree gold reads from isn't there at all.

    Raises FileNotFoundError; main() turns that into a non-zero exit.

    This exists because gold's per-feed behaviour on a missing partition is to
    print `SKIP — No partition at …` and carry on, which is CORRECT per feed --
    an alerts-only feed genuinely has no `vehicles/` partition, and agencies
    routinely skip most of their feeds. But it means a gold process pointed at
    an empty curated tree skips all ~204 agencies, prints a zero-row summary,
    and exits 0. That is precisely how the 2026-07-31 Step Functions stage-split
    failed: gold ran in its own Fargate task with fresh ephemeral disk, saw no
    silver, and silently no-op'd the whole fleet while reporting success (see
    the NOTE in terraform/rollup.tf).

    So the check is on the INPUT TREE, not on an aggregate of per-feed results:
    "every feed skipped" is ambiguous (an alerts-only agency does that
    legitimately), whereas "the silver root holds no feed partitions at all" is
    not -- PROVIDED the tree is a stable one gold did not itself produce.

    Callers must only apply this to a silver tree owned by someone else (see
    main(): it runs only when --silver-dir is given). Applied to the combined
    task's local curated_dir it false-positives, because agency_batch runs
    agencies concurrently and clean_agency_curated deletes each one's output as
    it ships -- so the shared tree is legitimately empty at arbitrary moments.
    That regressed BENTON_AREA_TRANSPORTATION (an alerts-only agency, so it
    writes no vehicles/ or trip_updates/ partition of its own) on 2026-09-03.
    """
    fs, base = curated_fs(silver_dir)
    if any(has_any(fs, f"{base}/{sub}") for sub in _SOURCE_SUBDIR.values()):
        return
    raise FileNotFoundError(
        f"no silver partitions under {silver_dir} "
        f"({', '.join(sorted(_SOURCE_SUBDIR.values()))}) — refusing to report "
        "success for a run that would skip every feed. Did rollup run, and is "
        "--silver-dir pointing at its output?"
    )


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    if args.normalize:
        return _run_normalize(args)
    silver_dir = args.silver_dir if args.silver_dir is not None else args.curated_dir
    if args.all_days and "://" in str(silver_dir):
        # discover_dates_for_dir still globs a local Path. Failing loudly beats
        # returning an empty date list, which would print a zero-row summary and
        # exit 0 -- the same silent no-op assert_silver_present exists to stop.
        print(
            "[gold] FATAL — --all-days needs a local --silver-dir; remote "
            f"partition discovery isn't implemented (got {silver_dir}). Pass "
            "--day instead, which is what the nightly batch uses.",
            file=sys.stderr,
        )
        return 1
    # Only when reading a tree gold did not produce. In the combined task
    # silver_dir IS curated_dir, which rollup fills in this same container and
    # clean_agency_curated concurrently empties -- see assert_silver_present.
    if args.silver_dir is not None:
        try:
            assert_silver_present(silver_dir)
        except FileNotFoundError as e:
            print(f"[gold] FATAL — {e}", file=sys.stderr)
            return 1
    feed_tz_map = load_feed_tz_map(args.config)
    if args.feed:
        feeds = args.feed
    else:
        # See superseded_by_trip_updates: an agency builds from one source.
        skip = superseded_by_trip_updates(args.config)
        feeds = [f for f in sorted(feed_tz_map) if f not in skip]
    base_dates = [args.day] if args.day else []
    gtfs_for = None if args.no_otp else _make_gtfs_resolver(args)

    totals = {
        "stop": 0,
        "route": 0,
        "events": 0,
        "adherence": 0,
        "on_time": 0,
        "segments": 0,
        "routes": 0,
    }
    for feed in feeds:
        tz_str = feed_tz_map.get(feed)
        if tz_str is None:
            print(f"[{feed}] not in {args.config} — skipping", file=sys.stderr)
            continue
        tz = ZoneInfo(tz_str)
        source = args.source or (
            "trip-updates" if feed in _TRIP_UPDATES_ONLY_FEEDS else "vehicles"
        )
        dates = set(base_dates)
        if args.all_days:
            dates |= set(discover_dates(feed, args.curated_dir, source))
        if not dates:
            print(f"[{feed}] no dates to process — skipping", file=sys.stderr)
            continue
        for day in sorted(dates):
            result = build_one(
                feed,
                day,
                tz,
                args.curated_dir,
                source,
                args.merge_gap_seconds,
                args.force,
                silver_dir=silver_dir,
                gtfs_for=gtfs_for,
                early_threshold_s=args.early_threshold_seconds,
                late_threshold_s=args.late_threshold_seconds,
            )
            if result is not None:
                for key in totals:
                    totals[key] += result[key]

    summary = (
        f"---\ntotal: {totals['stop']:,} stop-day rows, "
        f"{totals['route']:,} route-day rows, {totals['events']:,} events"
    )
    if gtfs_for is not None:
        otp_pct = (
            100 * totals["on_time"] / totals["adherence"]
            if totals["adherence"]
            else 0.0
        )
        summary += f", {totals['adherence']:,} adherence rows ({otp_pct:.1f}% on-time)"
        summary += f", {totals['segments']:,} segment-speed rows"
        summary += f", {totals['routes']:,} routes"
    print(summary)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
