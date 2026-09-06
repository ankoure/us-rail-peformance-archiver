"""Resolve a service date to a cached static GTFS zip via MobilityDatabase's API.

Prints the local zip path on success. No-op (just prints the path) if the
snapshot is already cached. Requires MDB_REFRESH_TOKEN in the environment.

Example:

    MDB_REFRESH_TOKEN=... uv run python scripts/fetch_static_gtfs.py \\
        --feed-id mdb-1847 --agency wmata --date 2026-05-20
"""

from __future__ import annotations

import argparse
import datetime as dt
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from analysis.gtfs_fetcher import (  # noqa: E402
    DEFAULT_API_URL,
    DEFAULT_CACHE_DIR,
    ensure_local_zip,
    fetch_catalog,
    pick_snapshot,
)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    p.add_argument(
        "--feed-id", required=True, help="MobilityDatabase feed ID, e.g. mdb-1847"
    )
    p.add_argument(
        "--agency", required=True, help="Short agency slug for cache path, e.g. wmata"
    )
    p.add_argument(
        "--date",
        required=True,
        type=dt.date.fromisoformat,
        help="Target service date YYYY-MM-DD",
    )
    p.add_argument(
        "--cache-dir",
        type=Path,
        default=DEFAULT_CACHE_DIR,
        help=f"Where to cache zips (default: {DEFAULT_CACHE_DIR})",
    )
    p.add_argument(
        "--api-url",
        default=DEFAULT_API_URL,
        help=f"MobilityDatabase API base URL (default: {DEFAULT_API_URL})",
    )
    return p.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    """Resolve the date to a snapshot, ensure its zip is cached, and print the path."""
    args = parse_args(argv)
    catalog = fetch_catalog(args.feed_id, api_url=args.api_url)
    snapshot = pick_snapshot(catalog, args.date)
    print(
        f"[catalog] {args.feed_id}: snapshot start={snapshot.feed_start_date} "
        f"end={snapshot.feed_end_date} version={snapshot.feed_version}",
        file=sys.stderr,
    )
    path = ensure_local_zip(snapshot, args.agency, cache_dir=args.cache_dir)
    print(path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
