"""Export gobble-style events.csv files for one or more feeds and days.

Per-feed timezones are looked up from config/feeds.yaml, so --tz is optional.
When --feed is omitted, every feed in the config is processed. When date args
are omitted, --all-days picks up every service_date partition that exists on
disk for each feed.

GTFS enrichment is automatic when an agency in feeds.yaml declares an
`mdb_feed_id`: every feed under that agency gets direction_id backfilled
from trips.txt and scheduled_headway / scheduled_tt joined from stop_times.
This is how feeds that omit direction_id from their realtime payload
(NYCT subway, TriMet, MetroSTL, UTA, PRT) produce non-empty events. Explicit
--gtfs or --mdb-feed-id args still override per-feed config but are
single-feed only.

Examples:

    # one day, one feed (tz auto-looked-up from feeds.yaml)
    uv run python scripts/export_events.py \\
        --feed wmata-vehicles --date 2026-05-20

    # multiple feeds, an inclusive range
    uv run python scripts/export_events.py \\
        --feed wmata-vehicles septa-rail-vehicles \\
        --start 2026-05-18 --end 2026-05-21

    # every feed in feeds.yaml, every available day on disk;
    # GTFS auto-fetched per agency from their mdb_feed_id
    uv run python scripts/export_events.py --all-days

    # every feed, a specific date (skips feeds with no data for that day)
    uv run python scripts/export_events.py --date 2026-05-20

    # explicit GTFS override (single feed only)
    uv run python scripts/export_events.py \\
        --feed wmata-vehicles --date 2026-05-20 \\
        --mdb-feed-id mdb-1849 --agency wmata

    # derive events from trip_updates instead of vehicle positions
    # (use for metromn-trips: LRT vehicle pings omit stop_id, so trip_updates
    # is the only path that produces Blue/Green Line events)
    uv run python scripts/export_events.py \\
        --feed metromn-trips --source trip-updates --date 2026-05-22
"""

from __future__ import annotations

import argparse
import datetime as dt
import re
import sys
from pathlib import Path
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

# Allow `from analysis import ...` when run as `python scripts/export_events.py`.
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from analysis import (  # noqa: E402
    GtfsResolver,
    StaticGtfs,
    TripUpdatesDay,
    VehicleDay,
    export_events_csv,
)
from analysis.gtfs_fetcher import DEFAULT_API_URL, DEFAULT_CACHE_DIR  # noqa: E402
from archiver.loader import load_config  # noqa: E402


_PART_RE = re.compile(r"^(year|month|day)=(\d+)$")


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """Parse argv and enforce the date / GTFS-override / single-feed rules."""
    p = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    p.add_argument(
        "--feed",
        nargs="+",
        default=None,
        help="One or more feed names, e.g. wmata-vehicles septa-rail-vehicles. "
        "Omit to process every feed defined in feeds.yaml.",
    )
    p.add_argument(
        "--tz",
        default=None,
        help="IANA timezone for the feed's local service date. Optional — "
        "when omitted, the timezone is looked up per-feed from feeds.yaml.",
    )
    p.add_argument(
        "--config",
        type=Path,
        default=Path("config/feeds.yaml"),
        help="Path to feeds.yaml (default: ./config/feeds.yaml). Used for "
        "feed→timezone lookup and the default feed list.",
    )
    p.add_argument(
        "--date",
        nargs="+",
        type=dt.date.fromisoformat,
        help="One or more YYYY-MM-DD dates",
    )
    p.add_argument(
        "--start", type=dt.date.fromisoformat, help="Inclusive range start (YYYY-MM-DD)"
    )
    p.add_argument(
        "--end", type=dt.date.fromisoformat, help="Inclusive range end (YYYY-MM-DD)"
    )
    p.add_argument(
        "--all-days",
        action="store_true",
        help="Use every service_date partition that exists on disk for each "
        "feed (unioned with any --date / --start..--end values).",
    )
    p.add_argument(
        "--curated-dir",
        type=Path,
        default=Path("data/curated"),
        help="Where to read vehicles parquet from and write events under (default: ./data/curated)",
    )
    p.add_argument(
        "--source",
        choices=["vehicles", "trip-updates"],
        default="vehicles",
        help="Which curated dataset to derive Visits from (default: vehicles). "
        "Use 'trip-updates' for feeds whose vehicle positions lack stop_id / "
        "current_status — e.g. point at the metromn-trips feed to capture "
        "Metro Transit MN light rail events that the vehicles feed omits.",
    )
    p.add_argument(
        "--merge-gap-seconds",
        type=int,
        default=60,
        help="Collapse same-stop visits separated by <= this many seconds (default: 60). "
        "Set to 0 to disable.",
    )
    p.add_argument(
        "--gtfs",
        type=Path,
        default=None,
        help="Path to a GTFS static zip. Single-feed only. If provided, "
        "scheduled_headway and scheduled_tt are populated by joining against "
        "this schedule.",
    )
    p.add_argument(
        "--mdb-feed-id",
        default=None,
        help="MobilityDatabase feed ID (e.g. mdb-1847). Single-feed only. "
        "When provided with --agency, the export auto-fetches the static GTFS "
        "snapshot effective for each date via MobilityDatabase's API (requires "
        "MDB_REFRESH_TOKEN in the environment). Mutually exclusive with --gtfs.",
    )
    p.add_argument(
        "--agency",
        default=None,
        help="Agency slug used for the cached zip path (e.g. wmata). Required with --mdb-feed-id.",
    )
    p.add_argument(
        "--gtfs-cache-dir",
        type=Path,
        default=DEFAULT_CACHE_DIR,
        help=f"Where to cache fetched GTFS zips (default: {DEFAULT_CACHE_DIR})",
    )
    p.add_argument(
        "--gtfs-api-url",
        default=DEFAULT_API_URL,
        help="MobilityDatabase API base URL",
    )
    args = p.parse_args(argv)

    if not args.date and not (args.start and args.end) and not args.all_days:
        p.error("provide --date YYYY-MM-DD [...] or --start..--end or --all-days")
    if (args.start and not args.end) or (args.end and not args.start):
        p.error("--start and --end must be given together")
    if args.gtfs and args.mdb_feed_id:
        p.error("--gtfs and --mdb-feed-id are mutually exclusive")
    if args.mdb_feed_id and not args.agency:
        p.error("--agency is required when using --mdb-feed-id")
    if (args.gtfs or args.mdb_feed_id) and (args.feed is None or len(args.feed) != 1):
        p.error("--gtfs / --mdb-feed-id require a single --feed")
    return args


def load_feed_tz_map(config_path: Path) -> dict[str, str]:
    """Return feed_name → IANA timezone, gathered from every agency."""
    config = load_config(str(config_path))
    return {
        feed.name: agency.timezone
        for agency in config.agencies
        for feed in agency.feeds
    }


def load_feed_agency_map(
    config_path: Path,
) -> dict[str, tuple[str, str | None]]:
    """Return feed_name → (agency_id, mdb_feed_id_or_None).

    Drives per-feed GTFS resolver construction in the default (no-override)
    path: every feed whose parent agency declares an mdb_feed_id gets its
    direction_id and scheduled_* fields enriched from the archived schedule.
    """
    config = load_config(str(config_path))
    return {
        feed.name: (agency.agency_id, agency.mdb_feed_id)
        for agency in config.agencies
        for feed in agency.feeds
    }


def resolve_feeds(
    args: argparse.Namespace, feed_tz_map: dict[str, str]
) -> list[tuple[str, ZoneInfo]]:
    """Pair each requested feed with its timezone."""
    if args.tz:
        try:
            override_tz = ZoneInfo(args.tz)
        except ZoneInfoNotFoundError:
            raise SystemExit(f"Unknown timezone: {args.tz!r}")
    else:
        override_tz = None

    names = args.feed if args.feed else sorted(feed_tz_map)
    pairs: list[tuple[str, ZoneInfo]] = []
    for name in names:
        if override_tz is not None:
            pairs.append((name, override_tz))
            continue
        tz_str = feed_tz_map.get(name)
        if tz_str is None:
            raise SystemExit(
                f"Feed {name!r} not in {args.config}; pass --tz to override "
                "or add the feed/agency to the config."
            )
        pairs.append((name, ZoneInfo(tz_str)))
    return pairs


def explicit_dates(args: argparse.Namespace) -> list[dt.date]:
    """Dates supplied via --date / --start..--end (no disk discovery)."""
    dates: list[dt.date] = []
    if args.date:
        dates.extend(args.date)
    if args.start and args.end:
        if args.end < args.start:
            raise SystemExit("--end must be >= --start")
        d = args.start
        while d <= args.end:
            dates.append(d)
            d += dt.timedelta(days=1)
    return sorted(set(dates))


_SOURCE_SUBDIR = {"vehicles": "vehicles", "trip-updates": "trip_updates"}


def discover_dates(
    feed: str, curated_dir: Path, source: str = "vehicles"
) -> list[dt.date]:
    """Every service_date that has a partition for `feed` under curated_dir.

    `source` selects which curated dataset to scan: 'vehicles' (default) or
    'trip-updates'. Returns an empty list when nothing's on disk for that
    (feed, source) — callers should treat absence as "skip this feed".
    """
    feed_root = curated_dir / _SOURCE_SUBDIR[source] / f"feed={feed}"
    if not feed_root.exists():
        return []
    found: list[dt.date] = []
    for day_dir in feed_root.glob("year=*/month=*/day=*"):
        parts: dict[str, int] = {}
        ok = True
        for part in day_dir.parts[-3:]:
            m = _PART_RE.match(part)
            if not m:
                ok = False
                break
            parts[m.group(1)] = int(m.group(2))
        if not ok or set(parts) != {"year", "month", "day"}:
            continue
        try:
            found.append(dt.date(parts["year"], parts["month"], parts["day"]))
        except ValueError:
            continue
    return sorted(set(found))


def resolve_dates_for_feed(
    args: argparse.Namespace, feed: str, base: list[dt.date]
) -> list[dt.date]:
    """Explicit dates, unioned with on-disk partitions when --all-days is set."""
    if not args.all_days:
        return base
    return sorted(set(base) | set(discover_dates(feed, args.curated_dir, args.source)))


def main(argv: list[str] | None = None) -> int:
    """Export events for every requested (feed, day); returns a process exit code.

    Picks the Visit source (vehicles vs trip_updates), resolves the right GTFS
    snapshot per (feed, day) — explicit override, else the agency's mdb_feed_id
    resolver — and prints per-feed and grand totals. Missing partitions and
    GTFS lookup failures skip that (feed, day) rather than abort the run.
    """
    args = parse_args(argv)
    feed_tz_map = load_feed_tz_map(args.config)
    feed_agency_map = load_feed_agency_map(args.config)
    feed_pairs = resolve_feeds(args, feed_tz_map)
    base_dates = explicit_dates(args)

    static_gtfs = StaticGtfs(args.gtfs) if args.gtfs else None
    override_resolver = (
        GtfsResolver(
            args.mdb_feed_id,
            args.agency,
            cache_dir=args.gtfs_cache_dir,
            api_url=args.gtfs_api_url,
        )
        if args.mdb_feed_id
        else None
    )
    # Default path: one resolver per agency, lazily built the first time we
    # need a snapshot for any of its feeds.
    resolver_cache: dict[str, GtfsResolver] = {}

    def gtfs_for(feed: str, d: dt.date) -> StaticGtfs | None:
        if static_gtfs is not None:
            return static_gtfs
        if override_resolver is not None:
            return override_resolver.for_date(d)
        info = feed_agency_map.get(feed)
        if info is None:
            return None
        agency_id, mdb_id = info
        if not mdb_id:
            return None
        if agency_id not in resolver_cache:
            resolver_cache[agency_id] = GtfsResolver(
                mdb_id,
                agency_id.lower(),
                cache_dir=args.gtfs_cache_dir,
                api_url=args.gtfs_api_url,
            )
        return resolver_cache[agency_id].for_date(d)

    grand_rows = 0
    grand_files = 0
    for feed, tz in feed_pairs:
        dates = resolve_dates_for_feed(args, feed, base_dates)
        if not dates:
            print(f"[{feed}] no dates to process — skipping", file=sys.stderr)
            continue
        feed_rows = 0
        feed_files = 0
        day_cls = VehicleDay if args.source == "vehicles" else TripUpdatesDay
        for d in dates:
            try:
                day = day_cls(
                    feed,
                    d,
                    base_dir=args.curated_dir,
                    merge_gap_seconds=args.merge_gap_seconds,
                )
                gtfs_for_day = gtfs_for(feed, d)
                rows, files = export_events_csv(
                    day, tz, base_dir=args.curated_dir, gtfs=gtfs_for_day
                )
            except FileNotFoundError as e:
                print(f"[{feed} {d}] SKIP — {e}", file=sys.stderr)
                continue
            except LookupError as e:
                print(f"[{feed} {d}] SKIP GTFS lookup — {e}", file=sys.stderr)
                continue
            print(f"[{feed} {d}] {rows:>8,} rows  {files:>5} files")
            feed_rows += rows
            feed_files += files
        print(f"[{feed}] subtotal: {feed_rows:,} rows across {feed_files} files")
        grand_rows += feed_rows
        grand_files += feed_files

    print(f"---\ntotal: {grand_rows:,} rows across {grand_files} files")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
