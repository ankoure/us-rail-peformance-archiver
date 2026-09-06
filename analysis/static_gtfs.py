"""Load a GTFS static zip and answer scheduled_tt / scheduled_headway queries.

The matching semantics follow TransitMatters' gobble (gtfs.py:add_gtfs_headways):

  scheduled_tt        = seconds elapsed from the trip's first scheduled
                        arrival to its arrival at this stop.
  scheduled_headway   = seconds between consecutive scheduled arrivals at
                        (route_id, direction_id, stop_id) on the same
                        service day.

Service-day filtering combines calendar.txt and calendar_dates.txt: a
service_id is active on a date iff (a) it falls within [start_date, end_date]
and the weekday flag is 1, **and** (b) calendar_dates.txt has no exception_type
== 2 ('removed') for that (service_id, date), and additionally any service_id
present in calendar_dates with exception_type == 1 ('added') on that date is
included.

The class is intentionally focused on what events.csv enrichment needs; it
is not a general GTFS query library.
"""

from __future__ import annotations

import datetime as dt
import zipfile
from functools import cached_property, lru_cache
from pathlib import Path

import pandas as pd

from analysis.geo import cumulative_arc_length_m, project_point_to_polyline

# GTFS calendar.txt has one column per weekday, named lower-case English.
_WEEKDAY_COLS = [
    "monday",
    "tuesday",
    "wednesday",
    "thursday",
    "friday",
    "saturday",
    "sunday",
]
_RTE_DIR_STOP = ["route_id", "direction_id", "stop_id"]


class StaticGtfs:
    """One GTFS static zip, read lazily and answering enrichment queries.

    Each component table (`trips`, `routes`, `stop_times`, `calendar`,
    `calendar_dates`) is read from the zip on first access and cached. Missing
    optional files degrade to empty frames rather than raising, so a feed that
    omits e.g. calendar_dates.txt still works. The zip is never extracted to
    disk — tables are read straight out of the archive.

    Three mutually-exclusive, optional ways to scope every table to a slice of
    a zip that covers MULTIPLE operators under one mdb_feed_id -- a single
    config agency never needs more than one of these at a time:

      `agency_prefix` matches `f"{agency_prefix}:"` against routes.txt's
      agency_id -- e.g. Entur's Norwegian national feed (mdb-1078), which
      GO_AHEAD's config feeds share even though each is really one operator's
      slice of it (GOA/RUT/VYB/VYX).

      `agency_ids` matches an exact agency_id SET -- for an operator (or small
      group of them) with no shared prefix, e.g. Transport for NSW's combined
      feed (mdb-2449, TFNSW_SYDNEY_TRAINS=x0001, TFNSW_LIGHT_RAIL={SLR,PLR,LR}).

      `route_types` matches an exact GTFS route_type SET -- for a config
      agency whose routes span hundreds of distinct, unenumerable agency_ids
      but share a mode, e.g. TFNSW_BUSES (route_type 700/712/714 across 644
      bus-operator agency_ids -- listing them individually isn't practical the
      way it is for the five-or-fewer-operator cases above).

    Confirmed 2026-09-06: Entur's shapes.txt alone is 38M rows / 2.57 GB and
    TfNSW's is 1.01 GB (423 MB stop_times.txt) -- reading either whole (the
    all-None/default path, unchanged for every other agency) OOMed a 20 GiB
    container before finishing parsing. When any filter is set, `routes`/
    `trips` filter after a normal full read (both small even for these two:
    routes.txt/trips.txt are KB-MB, not GB), then `shapes`/`stop_times` (the
    two big tables) filter WHILE reading, chunk by chunk, so peak memory is
    bounded by one chunk instead of the whole file -- filtering after a full
    read wouldn't help, since the unfiltered read is itself the peak. All-None
    (the default) reproduces the exact prior behavior for every agency that
    isn't a multi-operator aggregator.
    """

    def __init__(
        self,
        zip_path: Path | str,
        agency_prefix: str | None = None,
        agency_ids: frozenset[str] | None = None,
        route_types: frozenset[int] | None = None,
    ) -> None:
        self.zip_path = Path(zip_path)
        if not self.zip_path.exists():
            raise FileNotFoundError(self.zip_path)
        self.agency_prefix = agency_prefix
        self.agency_ids = agency_ids
        self.route_types = route_types

    @property
    def _scoped(self) -> bool:
        """Whether any of the three filters in the class docstring is active."""
        return bool(self.agency_prefix or self.agency_ids or self.route_types)

    def __repr__(self) -> str:
        return f"StaticGtfs({self.zip_path})"

    def _read(self, name: str, **read_csv_kwargs) -> pd.DataFrame:
        # skipinitialspace handles agencies (e.g. Metra) whose CSV headers
        # have a space after each delimiter — without it, columns end up
        # named " trip_id" instead of "trip_id".
        read_csv_kwargs.setdefault("skipinitialspace", True)
        with zipfile.ZipFile(self.zip_path) as z:
            with z.open(name) as f:
                return pd.read_csv(f, **read_csv_kwargs)

    def _read_chunked_filtered(
        self,
        name: str,
        *,
        filter_col: str,
        allowed: set,
        chunksize: int = 1_000_000,
        **read_csv_kwargs,
    ) -> pd.DataFrame:
        """Like `_read`, but keeps only rows whose `filter_col` is in
        `allowed`, filtering chunk-by-chunk so peak memory is bounded by one
        chunk instead of the whole file.

        Only used when `agency_prefix` scopes a multi-operator zip to a small
        slice of a huge table (shapes.txt/stop_times.txt) -- filtering AFTER a
        full read wouldn't help, since the unfiltered read is itself the peak
        (see the class docstring: 38M rows / 2.57 GB for Entur's national
        feed, confirmed to OOM a 20 GiB container before finishing parsing).
        """
        read_csv_kwargs.setdefault("skipinitialspace", True)
        read_csv_kwargs["chunksize"] = chunksize
        with zipfile.ZipFile(self.zip_path) as z:
            with z.open(name) as f:
                chunks = [
                    chunk[chunk[filter_col].isin(allowed)]
                    for chunk in pd.read_csv(f, **read_csv_kwargs)
                ]
        if not chunks:
            return pd.DataFrame(columns=list(read_csv_kwargs.get("dtype", {})))
        return pd.concat(chunks, ignore_index=True)

    @cached_property
    def trips(self) -> pd.DataFrame:
        """trips.txt. Required by the GTFS spec, but degrades to an empty
        frame rather than raising when absent, matching routes/stops/shapes —
        several callers (e.g. [[trip_directions]], [[trip_shapes]]) only need
        to check column presence, not have the file guaranteed to exist."""
        try:
            df = self._read(
                "trips.txt",
                usecols=lambda c: (
                    c
                    in {
                        "trip_id",
                        "route_id",
                        "service_id",
                        "direction_id",
                        "trip_short_name",
                        "shape_id",
                    }
                ),
                dtype={
                    "trip_id": str,
                    "route_id": str,
                    "service_id": str,
                    "trip_short_name": str,
                    "shape_id": str,
                },
            )
        except KeyError:
            return pd.DataFrame(
                columns=[
                    "trip_id",
                    "route_id",
                    "service_id",
                    "direction_id",
                    "trip_short_name",
                    "shape_id",
                ]
            )
        if self._scoped:
            # trips.txt has no agency_id or route_type of its own -- scope via
            # the already-filtered routes instead. Both tables are small
            # enough (tens of MB) to read whole even unfiltered; only the two
            # big tables below (shapes/stop_times) need chunked filtering.
            df = df[df["route_id"].isin(self.routes["route_id"])]
        return df

    @cached_property
    def routes(self) -> pd.DataFrame:
        try:
            df = self._read(
                "routes.txt",
                # agency_id is only ever used by the agency_prefix filter
                # below; harmless extra column for every other caller.
                usecols=lambda c: (
                    c
                    in {
                        "route_id",
                        "route_type",
                        "route_short_name",
                        "route_long_name",
                        "agency_id",
                    }
                ),
                dtype={
                    "route_id": str,
                    "route_short_name": str,
                    "route_long_name": str,
                    "agency_id": str,
                },
            )
        except KeyError:
            return pd.DataFrame(
                columns=[
                    "route_id",
                    "route_type",
                    "route_short_name",
                    "route_long_name",
                ]
            )
        if self.agency_prefix and "agency_id" in df.columns:
            df = df[df["agency_id"].str.startswith(f"{self.agency_prefix}:")]
        elif self.agency_ids and "agency_id" in df.columns:
            df = df[df["agency_id"].isin(self.agency_ids)]
        elif self.route_types and "route_type" in df.columns:
            df = df[
                pd.to_numeric(df["route_type"], errors="coerce").isin(self.route_types)
            ]
        return df

    @cached_property
    def stops(self) -> pd.DataFrame:
        """stops.txt. `parent_station` groups platform-level stops under their
        station (e.g. MBTA subway platforms under a `place-*` id) — optional
        per the GTFS spec, so a feed without it just gets an all-null column
        rather than a missing one."""
        try:
            return self._read(
                "stops.txt",
                usecols=lambda c: (
                    c
                    in {
                        "stop_id",
                        "stop_code",
                        "stop_name",
                        "stop_lat",
                        "stop_lon",
                        "parent_station",
                    }
                ),
                dtype={
                    "stop_id": str,
                    "stop_code": str,
                    "stop_name": str,
                    "parent_station": str,
                },
            )
        except KeyError:
            return pd.DataFrame(
                columns=[
                    "stop_id",
                    "stop_code",
                    "stop_name",
                    "stop_lat",
                    "stop_lon",
                    "parent_station",
                ]
            )

    @cached_property
    def shapes(self) -> pd.DataFrame:
        """shapes.txt — route geometry as ordered (lat, lon) points per shape_id.

        Optional per the GTFS spec (only required when trips.txt references
        shape_id); missing shapes.txt degrades to an empty frame rather than
        raising, matching routes/stops. shape_dist_traveled is itself an
        optional column within shapes.txt — `usecols` simply omits it when absent.

        When agency_prefix scopes this zip, shapes.txt is read chunk-by-chunk
        and filtered to just the (already-scoped) trips' shape_ids -- see
        _read_chunked_filtered's docstring for why filtering has to happen
        during the read, not after it.
        """
        usecols = lambda c: (  # noqa: E731
            c
            in {
                "shape_id",
                "shape_pt_lat",
                "shape_pt_lon",
                "shape_pt_sequence",
                "shape_dist_traveled",
            }
        )
        try:
            if self._scoped:
                relevant_shape_ids = set(self.trips["shape_id"].dropna())
                return self._read_chunked_filtered(
                    "shapes.txt",
                    filter_col="shape_id",
                    allowed=relevant_shape_ids,
                    usecols=usecols,
                    dtype={"shape_id": str},
                )
            return self._read(
                "shapes.txt",
                usecols=usecols,
                dtype={"shape_id": str},
            )
        except KeyError:
            return pd.DataFrame(
                columns=[
                    "shape_id",
                    "shape_pt_lat",
                    "shape_pt_lon",
                    "shape_pt_sequence",
                    "shape_dist_traveled",
                ]
            )

    @cached_property
    def route_patterns(self) -> pd.DataFrame:
        """route_patterns.txt — an MBTA GTFS extension, one row per route pattern.

        Groups a route+direction's trips into named, typicality-ranked
        patterns (typical/deviation/atypical/diversion), each with a
        representative_trip_id. Most non-MBTA feeds omit this file; missing
        route_patterns.txt degrades to an empty frame, matching routes/stops/shapes.
        """
        try:
            return self._read(
                "route_patterns.txt",
                usecols=lambda c: (
                    c
                    in {
                        "route_pattern_id",
                        "route_id",
                        "direction_id",
                        "route_pattern_name",
                        "route_pattern_typicality",
                        "representative_trip_id",
                    }
                ),
                dtype={
                    "route_pattern_id": str,
                    "route_id": str,
                    "route_pattern_name": str,
                    "representative_trip_id": str,
                },
            )
        except KeyError:
            return pd.DataFrame(
                columns=[
                    "route_pattern_id",
                    "route_id",
                    "direction_id",
                    "route_pattern_name",
                    "route_pattern_typicality",
                    "representative_trip_id",
                ]
            )

    @cached_property
    def directions(self) -> pd.DataFrame:
        """directions.txt — an MBTA GTFS extension: human-readable direction names.

        Maps (route_id, direction_id) -> a short direction label (e.g.
        "Outbound") and destination (e.g. "Alewife"). Most non-MBTA feeds
        omit this file; missing directions.txt degrades to an empty frame.
        """
        try:
            return self._read(
                "directions.txt",
                usecols=lambda c: (
                    c
                    in {
                        "route_id",
                        "direction_id",
                        "direction",
                        "direction_destination",
                    }
                ),
                dtype={"route_id": str, "direction": str, "direction_destination": str},
            )
        except KeyError:
            return pd.DataFrame(
                columns=[
                    "route_id",
                    "direction_id",
                    "direction",
                    "direction_destination",
                ]
            )

    @cached_property
    def checkpoints(self) -> pd.DataFrame:
        """checkpoints.txt — an MBTA GTFS extension: named reporting points.

        checkpoint_id -> checkpoint_name only. stop_times.txt rows may carry a
        checkpoint_id (exposed on [[stop_times]] when present) but this class
        doesn't derive a stop->checkpoint mapping from it, since checkpoint_id
        is per stop_time (per trip), not guaranteed stable per stop. Most
        non-MBTA feeds omit this file; missing checkpoints.txt degrades to an
        empty frame.
        """
        try:
            return self._read(
                "checkpoints.txt",
                usecols=lambda c: c in {"checkpoint_id", "checkpoint_name"},
                dtype={"checkpoint_id": str, "checkpoint_name": str},
            )
        except KeyError:
            return pd.DataFrame(columns=["checkpoint_id", "checkpoint_name"])

    @cached_property
    def stop_coords(self) -> dict[str, tuple[float, float]]:
        """Map stop_id → (latitude, longitude) from stops.txt.

        Stops with missing or non-finite coordinates are omitted. Used by the
        segment-speed mart to compute haversine distances between consecutive stops.
        """
        df = self.stops
        if "stop_lat" not in df.columns or "stop_lon" not in df.columns:
            return {}
        out: dict[str, tuple[float, float]] = {}
        for row in df.itertuples(index=False):
            sid = getattr(row, "stop_id", None)
            if not isinstance(sid, str) or not sid:
                continue
            lat_raw = getattr(row, "stop_lat", None)
            lon_raw = getattr(row, "stop_lon", None)
            if not (pd.notna(lat_raw) and pd.notna(lon_raw)):
                continue
            try:
                lat, lon = float(lat_raw), float(lon_raw)
            except (TypeError, ValueError):
                continue
            out[sid] = (lat, lon)
        return out

    @cached_property
    def trip_shapes(self) -> dict[str, str]:
        """Map trip_id -> shape_id from trips.txt.

        Used to look up the polyline a trip follows for shape-following
        distance (see [[shape_points]]). Returns {} when trips.txt is missing
        trip_id, or when shape_id is absent — it's optional per the GTFS spec
        and not every producer publishes it.
        """
        if "trip_id" not in self.trips.columns or "shape_id" not in self.trips.columns:
            return {}
        out: dict[str, str] = {}
        for row in self.trips.itertuples(index=False):
            trip_id = getattr(row, "trip_id", None)
            shape_id = getattr(row, "shape_id", None)
            if isinstance(trip_id, str) and isinstance(shape_id, str) and shape_id:
                out[trip_id] = shape_id
        return out

    @cached_property
    def shape_points(self) -> dict[str, list[tuple[float, float]]]:
        """Map shape_id -> ordered list of (lat, lon) points from shapes.txt.

        Points are sorted by shape_pt_sequence. Used with [[trip_shapes]] to
        project a stop onto its trip's shape polyline for shape-following
        distance instead of assuming a straight line between stations — see
        analysis.geo.project_point_to_polyline. Missing shapes.txt, or shapes
        with fewer than 2 usable points, degrade to an absent/empty entry
        rather than raising, matching [[stop_coords]].
        """
        df = self.shapes
        required = {"shape_id", "shape_pt_lat", "shape_pt_lon", "shape_pt_sequence"}
        if not required.issubset(df.columns):
            return {}
        df = df.dropna(subset=list(required)).sort_values(
            ["shape_id", "shape_pt_sequence"]
        )
        out: dict[str, list[tuple[float, float]]] = {}
        for row in df.itertuples(index=False):
            sid = getattr(row, "shape_id", None)
            if not isinstance(sid, str) or not sid:
                continue
            try:
                lat, lon = float(row.shape_pt_lat), float(row.shape_pt_lon)
            except (TypeError, ValueError):
                continue
            out.setdefault(sid, []).append((lat, lon))
        return out

    @cached_property
    def route_direction_shapes(self) -> dict[tuple[str, int], list[str]]:
        """Map (route_id, direction_id) -> every distinct shape_id its trips
        use, most-common first (ties broken by lowest shape_id).

        A route+direction commonly runs more than one shape — a branching
        line (e.g. MBTA Red Line's Ashmont/Braintree branches share a trunk
        but diverge) has one shape per branch even within a single
        direction_id, and collapsing to a single "canonical" shape would
        silently drop whichever branch has fewer trips. Callers that draw
        the route on a map should draw every shape in this list, not just
        the first. Empty when trips.txt lacks route_id, direction_id, or
        shape_id.
        """
        required = {"route_id", "direction_id", "shape_id"}
        if not required.issubset(self.trips.columns):
            return {}
        df = self.trips.dropna(subset=list(required))
        if df.empty:
            return {}
        counts = (
            df.groupby(["route_id", "direction_id", "shape_id"])
            .size()
            .reset_index(name="n")
            .sort_values(["n", "shape_id"], ascending=[False, True])
        )
        out: dict[tuple[str, int], list[str]] = {}
        for row in counts.itertuples(index=False):
            key = (row.route_id, int(row.direction_id))
            out.setdefault(key, []).append(row.shape_id)
        return out

    def route_direction_stop_offsets(
        self, route_id: str, direction_id: int
    ) -> dict[str, dict[str, float]]:
        """shape_id -> {stop_id -> distance (metres) along that shape}, for
        every shape this route+direction uses (see [[route_direction_shapes]])
        and every stop a trip running that exact shape visits.

        A stop shared by multiple branches (e.g. a trunk station before a
        Y-split) gets one entry per shape it actually appears on, each with
        that shape's own arc-length offset — a caller slicing a segment
        between two stops needs both stops resolved on the *same* shape, so
        it should try [[route_direction_shapes]]' shapes in order and use
        the first one covering both. A stop pair spanning two different
        shapes with no shared coverage (e.g. one endpoint on a branch the
        other doesn't reach) has no entry on any single shape; callers
        should fall back to straight-line stop coordinates for those, the
        same way [[shape_points]]'s consumer
        (analysis/segment_speed.py's `_shape_distance_m`) falls back to
        haversine whenever shape projection isn't available.
        """
        shape_ids = self.route_direction_shapes.get((route_id, direction_id), [])
        trips = self.trips
        out: dict[str, dict[str, float]] = {}
        for shape_id in shape_ids:
            points = self.shape_points.get(shape_id)
            if not points or len(points) < 2:
                continue
            cumulative = cumulative_arc_length_m(points)
            trip_ids = set(
                trips.loc[
                    (trips["route_id"] == route_id)
                    & (trips["direction_id"] == direction_id)
                    & (trips["shape_id"] == shape_id),
                    "trip_id",
                ]
            )
            stop_ids = set(
                self.stop_times.loc[
                    self.stop_times["trip_id"].isin(trip_ids), "stop_id"
                ]
            )
            offsets: dict[str, float] = {}
            for stop_id in stop_ids:
                coords = self.stop_coords.get(stop_id)
                if coords is None:
                    continue
                dist = project_point_to_polyline(*coords, points, cumulative)
                if dist is not None:
                    offsets[stop_id] = dist
            out[shape_id] = offsets
        return out

    @cached_property
    def route_modes(self) -> dict[str, str]:
        """Map route_id → mode bucket: 'rapid', 'bus', 'cr', or 'other'.

        Reads route_type from routes.txt and applies [[_categorize_route_type]],
        which accepts both the base GTFS values (0..12) and Google's extended
        Hierarchical Vehicle Types (100..1799). Used by the event exporter to
        partition output folders by mode without the caller having to know any
        GTFS route_type values.
        """
        df = self.routes
        if "route_id" not in df.columns or "route_type" not in df.columns:
            return {}
        out: dict[str, str] = {}
        for row in df.itertuples(index=False):
            rid = getattr(row, "route_id", None)
            if not isinstance(rid, str):
                continue
            raw = getattr(row, "route_type", None)
            try:
                rt: int | None = int(raw) if pd.notna(raw) else None
            except (TypeError, ValueError):
                rt = None
            out[rid] = _categorize_route_type(rt)
        return out

    @cached_property
    def trip_directions(self) -> dict[str, tuple[str | None, int | None]]:
        """Map trip_id → (route_id, direction_id) from trips.txt.

        Used to backfill the trip descriptor for GTFS-rt feeds (NYCT subway,
        TriMet, several CARTA/UTA/PRT feeds) that publish trip_id but omit
        direction_id — and sometimes route_id — in their realtime payload.

        Returns an empty map when the static feed's trips.txt is missing
        trip_id (some agencies publish a schema that drops required columns,
        or wraps headers in whitespace/BOM that breaks the column name).
        """
        if "trip_id" not in self.trips.columns:
            return {}
        out: dict[str, tuple[str | None, int | None]] = {}
        has_dir = "direction_id" in self.trips.columns
        has_route = "route_id" in self.trips.columns
        for row in self.trips.itertuples(index=False):
            trip_id = getattr(row, "trip_id", None)
            if not isinstance(trip_id, str):
                continue
            route_id = getattr(row, "route_id", None) if has_route else None
            if not isinstance(route_id, str):
                route_id = None
            direction_id: int | None = None
            if has_dir:
                raw = getattr(row, "direction_id", None)
                if pd.notna(raw):
                    direction_id = int(raw)
            out[trip_id] = (route_id, direction_id)
        return out

    @cached_property
    def direction_by_trip(self) -> dict[str, int]:
        """Map trip_id -> direction_id from trips.txt, omitting trips with no
        direction_id. A thin projection of [[trip_directions]] (drops the
        route_id half) for callers that key purely on trip_id — e.g.
        segment_speed.py's compute_segment_speeds, which uses this as the
        authoritative direction_id for a trip: some feeds' realtime trip
        descriptors report a direction_id that's out of range or
        inconsistent with the stop the vehicle is actually dwelling at
        (observed on GCRTA), while trips.txt's direction_id is exactly the
        grouping key route_shapes/route_shape_stops are built from, so
        preferring it keeps segment_day's direction_id in the same space the
        map's shape-matching queries against.
        """
        return {tid: d for tid, (_, d) in self.trip_directions.items() if d is not None}

    # ------------------------------------------------------------------
    # Resolution indexes — raw realtime identifier -> canonical GTFS id.
    # Ported from nibble's StaticGTFS (keep-first semantics): they let the
    # gold-layer normalizers resolve the raw tokens our JSON feeds land
    # (route short/long names, stop codes/names, train numbers).
    # ------------------------------------------------------------------

    @staticmethod
    def _keepfirst_index(
        df: pd.DataFrame,
        value_col: str,
        key_cols: tuple[str, ...],
        *,
        key_transform=None,
    ) -> dict[str, str]:
        """Build a keep-first ``{stripped key -> value}`` map from a GTFS table.

        Key columns are scanned in order, so an earlier column's keys take
        precedence over a later one's on collision (e.g. route_short_name beats
        route_long_name) regardless of row order. A row is kept only when both
        the value and the key are non-empty strings; ``key_transform`` (e.g.
        ``str.upper``) is applied to the stripped key. Returns {} when
        ``value_col`` is absent.
        """
        if value_col not in df.columns:
            return {}
        out: dict[str, str] = {}
        for col in key_cols:
            if col not in df.columns:
                continue
            for value, key in zip(df[value_col], df[col]):
                if not isinstance(value, str) or not value:
                    continue
                if isinstance(key, str) and key.strip():
                    k = key.strip()
                    out.setdefault(key_transform(k) if key_transform else k, value)
        return out

    @cached_property
    def route_short_names(self) -> dict[str, str]:
        """Map route_short_name AND route_long_name -> route_id.

        Indexing both names lets feeds that report either as their route
        identifier (MWRTA short codes, CCRTA/Passio long names, VTA headsigns)
        resolve through one dict. Precedence is independent of routes.txt row
        order: short names are indexed before long names, so a long name can
        never shadow another route's short name. Within a single tier, ties keep
        the first route in file order.
        """
        return self._keepfirst_index(
            self.routes, "route_id", ("route_short_name", "route_long_name")
        )

    @cached_property
    def stop_codes(self) -> dict[str, str]:
        """Map stop_code -> stop_id from stops.txt (LIRR-style raw codes)."""
        return self._keepfirst_index(self.stops, "stop_id", ("stop_code",))

    @cached_property
    def stop_names(self) -> dict[str, str]:
        """Map UPPER(stop_name) -> stop_id (MARTA station-name precedent)."""
        return self._keepfirst_index(
            self.stops, "stop_id", ("stop_name",), key_transform=str.upper
        )

    @cached_property
    def trip_short_names(self) -> dict[str, str]:
        """Map trip_short_name -> trip_id from trips.txt (LIRR train numbers)."""
        return self._keepfirst_index(self.trips, "trip_id", ("trip_short_name",))

    @cached_property
    def route_ids(self) -> set[str]:
        """Set of known GTFS route_ids — used to detect tokens that are already canonical."""
        df = self.routes
        if "route_id" not in df.columns:
            return set()
        return {r for r in df["route_id"] if isinstance(r, str) and r}

    @cached_property
    def stop_times(self) -> pd.DataFrame:
        """stop_times.txt plus derived `arrival_seconds` / `departure_seconds`.

        The HH:MM:SS time strings (which GTFS allows past 24:00 for next-day
        continuations) are converted to seconds via [[_hms_to_seconds]] so
        scheduled_tt and scheduled_headway are plain integer subtractions.

        Also exposes `checkpoint_id` (an MBTA extension column) when present,
        for callers that want to join to [[checkpoints]] themselves — this
        class doesn't derive a stop->checkpoint mapping, since checkpoint_id
        is per stop_time (per trip), not guaranteed stable per stop.

        When agency_prefix scopes this zip, stop_times.txt is read
        chunk-by-chunk and filtered to just the (already-scoped) trips' trip
        ids -- same reasoning as [[shapes]].
        """
        usecols = lambda c: (  # noqa: E731
            c
            in {
                "trip_id",
                "stop_sequence",
                "stop_id",
                "arrival_time",
                "departure_time",
                "checkpoint_id",
            }
        )
        dtype = {"trip_id": str, "stop_id": str, "checkpoint_id": str}
        if self._scoped:
            df = self._read_chunked_filtered(
                "stop_times.txt",
                filter_col="trip_id",
                allowed=set(self.trips["trip_id"]),
                usecols=usecols,
                dtype=dtype,
            )
        else:
            df = self._read("stop_times.txt", usecols=usecols, dtype=dtype)
        # GTFS allows times like "25:30:00" (next-day continuation). Convert to seconds-since-noon-of-service-date
        # following the GTFS convention (noon-12h is the practical reference; using midnight is fine for our use
        # since both event and schedule are subtracted from the same midnight).
        df["arrival_seconds"] = df["arrival_time"].map(_hms_to_seconds)
        df["departure_seconds"] = df["departure_time"].map(_hms_to_seconds)
        return df

    @cached_property
    def calendar(self) -> pd.DataFrame:
        try:
            df = self._read(
                "calendar.txt",
                dtype={"service_id": str},
                parse_dates=["start_date", "end_date"],
                date_format="%Y%m%d",
            )
        except (KeyError, ValueError):
            # KeyError: calendar.txt absent from the zip. ValueError: present
            # but missing a column parse_dates needs -- pandas raises "Missing
            # column provided to 'parse_dates'" rather than just leaving it
            # unparsed (observed on CERCANIAS: calendar.txt with no end_date
            # column). Either way, degrade to empty like the other optional
            # tables -- active_service_ids() still works off calendar_dates.txt.
            return pd.DataFrame(
                columns=["service_id", *_WEEKDAY_COLS, "start_date", "end_date"]
            )
        return df

    @cached_property
    def calendar_dates(self) -> pd.DataFrame:
        try:
            df = self._read(
                "calendar_dates.txt",
                dtype={"service_id": str},
                parse_dates=["date"],
                date_format="%Y%m%d",
            )
        except (KeyError, ValueError):
            return pd.DataFrame(columns=["service_id", "date", "exception_type"])
        return df

    def active_service_ids(self, service_date: dt.date) -> set[str]:
        """Service_ids active on the given local service date."""
        active: set[str] = set()
        target = pd.Timestamp(service_date)
        weekday_col = _WEEKDAY_COLS[service_date.weekday()]
        if not self.calendar.empty:
            cal = self.calendar
            mask = (
                (cal[weekday_col] == 1)
                & (cal["start_date"] <= target)
                & (cal["end_date"] >= target)
            )
            active.update(cal.loc[mask, "service_id"])
        if not self.calendar_dates.empty:
            cd = self.calendar_dates
            on_date = cd[cd["date"] == target]
            added = on_date[on_date["exception_type"] == 1]["service_id"]
            removed = on_date[on_date["exception_type"] == 2]["service_id"]
            active.update(added)
            active.difference_update(removed)
        return active

    @lru_cache(maxsize=8)
    def scheduled_stops(self, service_date: dt.date) -> pd.DataFrame:
        """Joined stop_times × trips for one service day, with scheduled_tt and scheduled_headway."""
        empty_cols = [
            "trip_id",
            "stop_sequence",
            "stop_id",
            "arrival_time",
            "departure_time",
            "arrival_seconds",
            "departure_seconds",
            "route_id",
            "direction_id",
            "scheduled_tt",
            "scheduled_headway",
        ]
        services = self.active_service_ids(service_date)
        if not services:
            return pd.DataFrame(columns=empty_cols)
        trips_today = self.trips[self.trips["service_id"].isin(services)]
        has_dir = "direction_id" in trips_today.columns
        trip_cols = ["trip_id", "route_id"] + (["direction_id"] if has_dir else [])
        joined = self.stop_times.merge(
            trips_today[trip_cols],
            on="trip_id",
            how="inner",
        )
        if not has_dir:
            # direction_id is optional in GTFS -- some agencies (observed:
            # DELFI, Kingston Transit, Thunder Bay Transit) omit it from
            # trips.txt entirely. Treat every trip at a stop as one direction
            # rather than crashing here: _RTE_DIR_STOP below groups by it for
            # scheduled_headway, and a NaN group key risks pandas' groupby
            # silently dropping those rows depending on version/dropna.
            joined["direction_id"] = 0
        if joined.empty:
            return pd.DataFrame(columns=empty_cols)

        # scheduled_tt: arrival_seconds minus the trip's first-stop arrival_seconds.
        first_arrival = joined.groupby("trip_id")["arrival_seconds"].transform("min")
        joined["scheduled_tt"] = (joined["arrival_seconds"] - first_arrival).astype(
            "Int64"
        )

        # scheduled_headway: per (route, dir, stop), diff between consecutive scheduled arrivals.
        joined = joined.sort_values(_RTE_DIR_STOP + ["arrival_seconds"]).reset_index(
            drop=True
        )
        joined["scheduled_headway"] = (
            joined.groupby(_RTE_DIR_STOP)["arrival_seconds"].diff().astype("Int64")
        )
        return joined

    def enrich_events(
        self,
        events: list[dict],
        service_date: dt.date,
    ) -> None:
        """Populate `scheduled_headway` and `scheduled_tt` on each event dict in-place.

        `events` must all share the same service_date (caller buckets first).
        Each event dict must have at minimum: trip_id, stop_id. Both fields are
        properties of the scheduled trip at that stop, so the lookup is a direct
        merge on (trip_id, stop_id) — using the actual event time would attribute
        a neighboring trip's values whenever the vehicle ran early or late.
        """
        if not events:
            return
        sched = self.scheduled_stops(service_date)
        if sched.empty:
            return

        lookup = (
            sched[["trip_id", "stop_id", "scheduled_tt", "scheduled_headway"]]
            .drop_duplicates(["trip_id", "stop_id"], keep="first")
            .set_index(["trip_id", "stop_id"])
            .to_dict("index")
        )
        for ev in events:
            row = lookup.get((ev.get("trip_id"), ev.get("stop_id")))
            if row is None:
                continue
            tt = row["scheduled_tt"]
            hw = row["scheduled_headway"]
            ev["scheduled_tt"] = "" if pd.isna(tt) else int(tt)
            ev["scheduled_headway"] = "" if pd.isna(hw) else int(hw)


_BASE_ROUTE_TYPES: dict[int, str] = {
    0: "rapid",  # Tram, Streetcar, Light rail
    1: "rapid",  # Subway, Metro
    2: "cr",  # Rail (intercity / commuter)
    3: "bus",
    5: "rapid",  # Cable tram
    11: "bus",  # Trolleybus
    12: "rapid",  # Monorail
}


def _categorize_route_type(rt: int | None) -> str:
    """Bucket a GTFS route_type into 'rapid', 'bus', 'cr', or 'other'.

    Recognizes both base GTFS route_types and Google's extended Hierarchical
    Vehicle Type ranges (RouteType reference at
    developers.google.com/transit/gtfs/reference/extended-route-types):
      100..117  Railway service (intercity / regional) → cr
      200..209  Coach service                          → bus
      300..307  Suburban Railway                       → cr
      400..405  Urban Railway / Metro                  → rapid
      700..716  Bus service                            → bus
      800       Trolleybus                             → bus
      900..906  Tram service                           → rapid
    Anything else (ferry, aerial lift, taxi, ...) falls into 'other'.
    """
    if rt is None:
        return "other"
    base = _BASE_ROUTE_TYPES.get(rt)
    if base is not None:
        return base
    if 100 <= rt <= 117 or 300 <= rt <= 307:
        return "cr"
    if 200 <= rt <= 209 or 700 <= rt <= 716 or rt == 800:
        return "bus"
    if 400 <= rt <= 405 or 900 <= rt <= 906:
        return "rapid"
    return "other"


def _hms_to_seconds(s: str) -> int:
    """Parse 'H:MM:SS' or 'HH:MM:SS' (allows hours >= 24) to total seconds.

    Returns -1 for missing/invalid values so the column stays int.
    """
    if not isinstance(s, str):
        return -1
    parts = s.split(":")
    if len(parts) != 3:
        return -1
    try:
        h, m, sec = (int(p) for p in parts)
    except ValueError:
        return -1
    return h * 3600 + m * 60 + sec
