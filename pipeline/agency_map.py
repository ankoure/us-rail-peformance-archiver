"""feed_name -> (agency_id, mdb_feed_id) grouping, shared by gold.py, gtfs.py, and
agency_batch.py. Hoisted out of gold.py/gtfs.py, where it was duplicated verbatim.
"""

from __future__ import annotations

from pathlib import Path

from archiver.loader import load_config


def load_feed_agency_map(config_path: str | Path) -> dict[str, tuple[str, str | None]]:
    """feed_name -> (agency_id, mdb_feed_id_or_None) for GTFS-snapshot resolution.

    A feed's parent agency supplies both the cache-path slug (agency_id) and the
    archived-feeds catalog id (mdb_feed_id).
    """
    config = load_config(str(config_path))
    return {
        feed.name: (agency.agency_id, agency.mdb_feed_id)
        for agency in config.agencies
        for feed in agency.feeds
    }


def load_feed_gtfs_scope(config_path: str | Path) -> dict[str, dict]:
    """feed_name -> GtfsResolver.for_date scoping kwargs, for feeds whose
    static schedule (the agency-level mdb_feed_id above) covers multiple
    operators and so needs scoping to just this feed's own one (see
    FeedConfig.gtfs_agency_prefix/gtfs_agency_ids/gtfs_route_types and
    StaticGtfs's docstring for the three ways a feed can be scoped, and why
    a single feed only ever needs one of them). Feeds needing none of the
    three are simply absent from the returned dict -- gtfs.py's
    `.get(feed, {})` then reads as "no scoping needed", matching every feed's
    behavior before this existed.

    Kept separate from load_feed_agency_map rather than widening its tuple:
    that function's 2-tuple shape is unpacked positionally at several call
    sites in gold.py, which has no use for any of this.
    """
    config = load_config(str(config_path))
    scopes: dict[str, dict] = {}
    for agency in config.agencies:
        for feed in agency.feeds:
            scope = {}
            if feed.gtfs_agency_prefix:
                scope["agency_prefix"] = feed.gtfs_agency_prefix
            if feed.gtfs_agency_ids:
                scope["agency_ids"] = frozenset(feed.gtfs_agency_ids)
            if feed.gtfs_route_types:
                scope["route_types"] = frozenset(feed.gtfs_route_types)
            if scope:
                scopes[feed.name] = scope
    return scopes
