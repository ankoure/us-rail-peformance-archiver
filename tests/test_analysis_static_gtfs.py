"""Tests for analysis/static_gtfs.py.

Builds tiny in-memory GTFS zips covering exactly the cases each test exercises.
"""

from __future__ import annotations

import datetime as dt
import zipfile
from pathlib import Path
from zoneinfo import ZoneInfo

import pytest

from analysis.static_gtfs import StaticGtfs, _categorize_route_type, _hms_to_seconds


def build_gtfs_zip(
    tmp_path: Path,
    calendar: str | None = None,
    calendar_dates: str | None = None,
    trips: str | None = None,
    stop_times: str | None = None,
    routes: str | None = None,
    stops: str | None = None,
    shapes: str | None = None,
    route_patterns: str | None = None,
    directions: str | None = None,
    checkpoints: str | None = None,
) -> Path:
    """Write a minimal GTFS zip with whatever tables the caller cares to specify."""
    zip_path = tmp_path / "feed.zip"
    with zipfile.ZipFile(zip_path, "w") as z:
        if calendar is not None:
            z.writestr("calendar.txt", calendar)
        if calendar_dates is not None:
            z.writestr("calendar_dates.txt", calendar_dates)
        if trips is not None:
            z.writestr("trips.txt", trips)
        if stop_times is not None:
            z.writestr("stop_times.txt", stop_times)
        if routes is not None:
            z.writestr("routes.txt", routes)
        if stops is not None:
            z.writestr("stops.txt", stops)
        if shapes is not None:
            z.writestr("shapes.txt", shapes)
        if route_patterns is not None:
            z.writestr("route_patterns.txt", route_patterns)
        if directions is not None:
            z.writestr("directions.txt", directions)
        if checkpoints is not None:
            z.writestr("checkpoints.txt", checkpoints)
    return zip_path


class TestResolutionIndexes:
    """The raw-identifier -> canonical-GTFS-id indexes used by the gold normalizers."""

    def test_route_short_names_indexes_short_and_long(self, tmp_path):
        routes = (
            "route_id,route_type,route_short_name,route_long_name\n"
            "r1,3,06,Sealine Hyannis-Falmouth\n"
            "r2,3,07,Barnstable Villager\n"
        )
        gtfs = StaticGtfs(build_gtfs_zip(tmp_path, routes=routes))
        idx = gtfs.route_short_names
        assert idx["06"] == "r1"
        assert idx["Sealine Hyannis-Falmouth"] == "r1"
        assert idx["07"] == "r2"

    def test_route_short_names_keep_first_on_duplicate(self, tmp_path):
        routes = (
            "route_id,route_type,route_short_name,route_long_name\n"
            "r1,3,X,First\n"
            "r2,3,X,Second\n"
        )
        gtfs = StaticGtfs(build_gtfs_zip(tmp_path, routes=routes))
        assert gtfs.route_short_names["X"] == "r1"  # first wins

    def test_route_short_names_short_beats_colliding_long(self, tmp_path):
        # A long name that collides with another route's short name must not
        # shadow it, even when the long name comes FIRST in file order.
        routes = (
            "route_id,route_type,route_short_name,route_long_name\n"
            "r2,3,Crosstown,9\n"  # r2's LONG name "9" appears first
            "r1,3,9,Downtown\n"  # r1's SHORT name "9"
        )
        gtfs = StaticGtfs(build_gtfs_zip(tmp_path, routes=routes))
        # "9" is r1's short name -> resolves to r1, not r2's long-name collision.
        assert gtfs.route_short_names["9"] == "r1"

    def test_indexes_empty_when_files_absent(self, tmp_path):
        gtfs = StaticGtfs(build_gtfs_zip(tmp_path))  # no routes/stops/trips
        assert gtfs.route_short_names == {}
        assert gtfs.stop_codes == {}
        assert gtfs.stop_names == {}

    def test_stop_codes_and_names(self, tmp_path):
        stops = "stop_id,stop_code,stop_name\nS1,0NY,Penn Station\nS2,BSR,Bay Shore\n"
        gtfs = StaticGtfs(build_gtfs_zip(tmp_path, stops=stops))
        assert gtfs.stop_codes == {"0NY": "S1", "BSR": "S2"}
        # stop_names keyed UPPERCASE
        assert gtfs.stop_names["PENN STATION"] == "S1"
        assert gtfs.stop_names["BAY SHORE"] == "S2"

    def test_index_drops_rows_with_empty_id(self, tmp_path):
        # A row whose canonical id is empty is never a usable mapping target.
        stops = "stop_id,stop_code,stop_name\n,BAD,Nowhere\nS2,BSR,Bay Shore\n"
        gtfs = StaticGtfs(build_gtfs_zip(tmp_path, stops=stops))
        assert gtfs.stop_codes == {"BSR": "S2"}  # empty-stop_id row dropped
        assert "NOWHERE" not in gtfs.stop_names

    def test_trip_short_names(self, tmp_path):
        trips = (
            "trip_id,route_id,service_id,direction_id,trip_short_name\n"
            "T100,r1,WK,0,64\n"
            "T200,r2,WK,1,8400\n"
        )
        gtfs = StaticGtfs(build_gtfs_zip(tmp_path, trips=trips))
        assert gtfs.trip_short_names == {"64": "T100", "8400": "T200"}

    def test_route_modes_still_works_after_widening(self, tmp_path):
        # Regression: widening routes usecols must not break route_modes.
        routes = (
            "route_id,route_type,route_short_name,route_long_name\n"
            "r1,2,CR,Commuter Rail\n"
        )
        gtfs = StaticGtfs(build_gtfs_zip(tmp_path, routes=routes))
        assert gtfs.route_modes == {"r1": "cr"}


# Convenience constants used across tests
EASTERN = ZoneInfo("America/New_York")


class TestHmsToSeconds:
    def test_normal_time(self):
        assert _hms_to_seconds("06:30:45") == 6 * 3600 + 30 * 60 + 45

    def test_after_midnight_continuation(self):
        # GTFS allows hours >= 24 for next-day continuations
        assert _hms_to_seconds("25:30:00") == 25 * 3600 + 30 * 60

    def test_invalid_returns_sentinel(self):
        assert _hms_to_seconds("garbage") == -1
        assert _hms_to_seconds("") == -1
        assert _hms_to_seconds(None) == -1  # type: ignore[arg-type]


class TestActiveServiceIds:
    def test_weekday_window(self, tmp_path):
        # Service WEEKDAY runs Mon-Fri 2026-05-01 through 2026-05-31
        calendar = (
            "service_id,monday,tuesday,wednesday,thursday,friday,saturday,sunday,start_date,end_date\n"
            "WEEKDAY,1,1,1,1,1,0,0,20260501,20260531\n"
            "WEEKEND,0,0,0,0,0,1,1,20260501,20260531\n"
        )
        gtfs = StaticGtfs(build_gtfs_zip(tmp_path, calendar=calendar))
        # 2026-05-20 is a Wednesday
        assert gtfs.active_service_ids(dt.date(2026, 5, 20)) == {"WEEKDAY"}
        # 2026-05-23 is a Saturday
        assert gtfs.active_service_ids(dt.date(2026, 5, 23)) == {"WEEKEND"}
        # Outside the window
        assert gtfs.active_service_ids(dt.date(2026, 6, 1)) == set()

    def test_calendar_dates_addition(self, tmp_path):
        calendar = (
            "service_id,monday,tuesday,wednesday,thursday,friday,saturday,sunday,start_date,end_date\n"
            "WEEKDAY,1,1,1,1,1,0,0,20260501,20260531\n"
        )
        # Add HOLIDAY service for one specific Saturday
        calendar_dates = "service_id,date,exception_type\nHOLIDAY,20260523,1\n"
        gtfs = StaticGtfs(
            build_gtfs_zip(tmp_path, calendar=calendar, calendar_dates=calendar_dates)
        )
        assert gtfs.active_service_ids(dt.date(2026, 5, 23)) == {"HOLIDAY"}

    def test_calendar_dates_removal(self, tmp_path):
        calendar = (
            "service_id,monday,tuesday,wednesday,thursday,friday,saturday,sunday,start_date,end_date\n"
            "WEEKDAY,1,1,1,1,1,0,0,20260501,20260531\n"
        )
        # Remove weekday service for Memorial Day (Mon 2026-05-25)
        calendar_dates = "service_id,date,exception_type\nWEEKDAY,20260525,2\n"
        gtfs = StaticGtfs(
            build_gtfs_zip(tmp_path, calendar=calendar, calendar_dates=calendar_dates)
        )
        assert gtfs.active_service_ids(dt.date(2026, 5, 25)) == set()

    def test_no_calendar_dates_file(self, tmp_path):
        calendar = (
            "service_id,monday,tuesday,wednesday,thursday,friday,saturday,sunday,start_date,end_date\n"
            "WEEKDAY,1,1,1,1,1,0,0,20260501,20260531\n"
        )
        gtfs = StaticGtfs(build_gtfs_zip(tmp_path, calendar=calendar))
        assert gtfs.active_service_ids(dt.date(2026, 5, 20)) == {"WEEKDAY"}

    def test_calendar_missing_end_date_column_degrades_to_empty(self, tmp_path):
        # Observed on CERCANIAS: calendar.txt present but without an end_date
        # column. pandas' read_csv(parse_dates=[...]) raises ValueError for a
        # named column that isn't there (not KeyError, which only covers the
        # whole file being absent from the zip) -- this used to propagate and
        # kill the whole gold.py run for the feed/day.
        calendar = (
            "service_id,monday,tuesday,wednesday,thursday,friday,saturday,sunday,start_date\n"
            "WEEKDAY,1,1,1,1,1,0,0,20260501\n"
        )
        calendar_dates = "service_id,date,exception_type\nHOLIDAY,20260520,1\n"
        gtfs = StaticGtfs(
            build_gtfs_zip(tmp_path, calendar=calendar, calendar_dates=calendar_dates)
        )
        assert gtfs.calendar.empty
        # calendar.txt degrading to empty shouldn't take calendar_dates.txt
        # down with it -- service should still come from the exception file.
        assert gtfs.active_service_ids(dt.date(2026, 5, 20)) == {"HOLIDAY"}


class TestScheduledStops:
    @pytest.fixture
    def simple_gtfs(self, tmp_path) -> StaticGtfs:
        calendar = (
            "service_id,monday,tuesday,wednesday,thursday,friday,saturday,sunday,start_date,end_date\n"
            "WD,1,1,1,1,1,0,0,20260501,20260531\n"
        )
        # Route R, direction 0. Three trips at this stop A, every 10 min.
        # Trip T1 visits stops A, B, C with travel times 0 / 5min / 12min from start.
        trips = (
            "route_id,service_id,trip_id,direction_id\n"
            "R,WD,T1,0\n"
            "R,WD,T2,0\n"
            "R,WD,T3,0\n"
        )
        stop_times = (
            "trip_id,arrival_time,departure_time,stop_id,stop_sequence\n"
            "T1,06:00:00,06:00:30,A,1\n"
            "T1,06:05:00,06:05:30,B,2\n"
            "T1,06:12:00,06:12:30,C,3\n"
            "T2,06:10:00,06:10:30,A,1\n"
            "T2,06:15:00,06:15:30,B,2\n"
            "T2,06:22:00,06:22:30,C,3\n"
            "T3,06:20:00,06:20:30,A,1\n"
            "T3,06:25:00,06:25:30,B,2\n"
            "T3,06:32:00,06:32:30,C,3\n"
        )
        return StaticGtfs(
            build_gtfs_zip(
                tmp_path, calendar=calendar, trips=trips, stop_times=stop_times
            )
        )

    def test_scheduled_tt_is_cumulative_from_trip_start(self, simple_gtfs):
        sched = simple_gtfs.scheduled_stops(dt.date(2026, 5, 20))
        t1 = sched[sched["trip_id"] == "T1"].sort_values("stop_sequence")
        assert list(t1["scheduled_tt"]) == [0, 5 * 60, 12 * 60]

    def test_scheduled_headway_is_per_route_dir_stop(self, simple_gtfs):
        sched = simple_gtfs.scheduled_stops(dt.date(2026, 5, 20))
        stop_a = sched[sched["stop_id"] == "A"].sort_values("arrival_seconds")
        # First trip has NaN headway, then 10 min, then 10 min
        headways = stop_a["scheduled_headway"].tolist()
        assert headways[0] is pd.NA
        assert headways[1:] == [10 * 60, 10 * 60]

    def test_returns_empty_on_inactive_date(self, simple_gtfs):
        # Saturday is outside the WD service
        assert simple_gtfs.scheduled_stops(dt.date(2026, 5, 23)).empty

    def test_missing_direction_id_column_does_not_crash(self, tmp_path):
        # direction_id is GTFS-optional. Observed omitted entirely on DELFI,
        # Kingston Transit, and Thunder Bay Transit's trips.txt -- used to
        # raise KeyError selecting trips_today[[..., "direction_id"]] and
        # kill the whole gold.py run for the feed/day.
        calendar = (
            "service_id,monday,tuesday,wednesday,thursday,friday,saturday,sunday,start_date,end_date\n"
            "WD,1,1,1,1,1,0,0,20260501,20260531\n"
        )
        trips = "route_id,service_id,trip_id\nR,WD,T1\nR,WD,T2\n"
        stop_times = (
            "trip_id,arrival_time,departure_time,stop_id,stop_sequence\n"
            "T1,06:00:00,06:00:30,A,1\n"
            "T2,06:10:00,06:10:30,A,1\n"
        )
        gtfs = StaticGtfs(
            build_gtfs_zip(
                tmp_path, calendar=calendar, trips=trips, stop_times=stop_times
            )
        )
        sched = gtfs.scheduled_stops(dt.date(2026, 5, 20))
        assert not sched.empty
        assert (sched["direction_id"] == 0).all()
        # Both trips collapse into direction_id=0, so headway is still
        # computed across them (not silently dropped by a NaN group key).
        stop_a = sched.sort_values("arrival_seconds")
        assert stop_a["scheduled_headway"].tolist() == [pd.NA, 10 * 60]


class TestStops:
    def test_exposes_parent_station_when_present(self, tmp_path):
        stops = (
            "stop_id,stop_name,stop_lat,stop_lon,parent_station\n"
            "70061,Alewife,42.39,-71.14,place-alfcl\n"
        )
        gtfs = StaticGtfs(build_gtfs_zip(tmp_path, stops=stops))
        assert gtfs.stops["parent_station"].tolist() == ["place-alfcl"]

    def test_missing_parent_station_column_is_omitted(self, tmp_path):
        # parent_station is itself GTFS-optional on stops.txt — matches
        # shape_dist_traveled's precedent: omitted, not backfilled with null.
        stops = "stop_id,stop_name,stop_lat,stop_lon\n70061,Alewife,42.39,-71.14\n"
        gtfs = StaticGtfs(build_gtfs_zip(tmp_path, stops=stops))
        assert "parent_station" not in gtfs.stops.columns

    def test_absent_file_degrades_to_empty_frame(self, tmp_path):
        gtfs = StaticGtfs(build_gtfs_zip(tmp_path))  # no stops.txt at all
        df = gtfs.stops
        assert df.empty
        assert "parent_station" in df.columns


class TestShapes:
    _COLUMNS = [
        "shape_id",
        "shape_pt_lat",
        "shape_pt_lon",
        "shape_pt_sequence",
        "shape_dist_traveled",
    ]

    def test_reads_points_in_order(self, tmp_path):
        shapes = (
            "shape_id,shape_pt_lat,shape_pt_lon,shape_pt_sequence,shape_dist_traveled\n"
            "S1,42.35,-71.06,1,0.0\n"
            "S1,42.36,-71.05,2,120.5\n"
        )
        gtfs = StaticGtfs(build_gtfs_zip(tmp_path, shapes=shapes))
        df = gtfs.shapes
        assert list(df.columns) == self._COLUMNS
        assert df["shape_id"].tolist() == ["S1", "S1"]
        assert df["shape_pt_sequence"].tolist() == [1, 2]

    def test_absent_file_degrades_to_empty_frame(self, tmp_path):
        gtfs = StaticGtfs(build_gtfs_zip(tmp_path))  # no shapes.txt at all
        df = gtfs.shapes
        assert df.empty
        assert list(df.columns) == self._COLUMNS

    def test_missing_shape_dist_traveled_column(self, tmp_path):
        # shape_dist_traveled is itself GTFS-optional within shapes.txt.
        shapes = (
            "shape_id,shape_pt_lat,shape_pt_lon,shape_pt_sequence\nS1,42.35,-71.06,1\n"
        )
        gtfs = StaticGtfs(build_gtfs_zip(tmp_path, shapes=shapes))
        df = gtfs.shapes
        assert "shape_dist_traveled" not in df.columns
        assert df["shape_id"].tolist() == ["S1"]


class TestMbtaExtensions:
    """route_patterns.txt / directions.txt / checkpoints.txt — MBTA GTFS extensions."""

    def test_route_patterns(self, tmp_path):
        route_patterns = (
            "route_pattern_id,route_id,direction_id,route_pattern_name,"
            "route_pattern_typicality,representative_trip_id\n"
            "R-1-0,R,0,Harvard - Nubian,1,T1\n"
        )
        gtfs = StaticGtfs(build_gtfs_zip(tmp_path, route_patterns=route_patterns))
        df = gtfs.route_patterns
        assert df["route_pattern_id"].tolist() == ["R-1-0"]
        assert df["route_pattern_typicality"].tolist() == [1]
        assert df["representative_trip_id"].tolist() == ["T1"]

    def test_route_patterns_absent_degrades_to_empty(self, tmp_path):
        gtfs = StaticGtfs(build_gtfs_zip(tmp_path))
        df = gtfs.route_patterns
        assert df.empty
        assert "representative_trip_id" in df.columns

    def test_directions(self, tmp_path):
        directions = (
            "route_id,direction_id,direction,direction_destination\n"
            "R,0,Outbound,Alewife\nR,1,Inbound,Braintree\n"
        )
        gtfs = StaticGtfs(build_gtfs_zip(tmp_path, directions=directions))
        df = gtfs.directions
        assert df["direction"].tolist() == ["Outbound", "Inbound"]
        assert df["direction_destination"].tolist() == ["Alewife", "Braintree"]

    def test_directions_absent_degrades_to_empty(self, tmp_path):
        gtfs = StaticGtfs(build_gtfs_zip(tmp_path))
        df = gtfs.directions
        assert df.empty
        assert list(df.columns) == [
            "route_id",
            "direction_id",
            "direction",
            "direction_destination",
        ]

    def test_checkpoints(self, tmp_path):
        checkpoints = (
            "checkpoint_id,checkpoint_name\nHARSQ,Harvard Square\nNUBN,Nubian\n"
        )
        gtfs = StaticGtfs(build_gtfs_zip(tmp_path, checkpoints=checkpoints))
        df = gtfs.checkpoints
        assert df["checkpoint_id"].tolist() == ["HARSQ", "NUBN"]
        assert df["checkpoint_name"].tolist() == ["Harvard Square", "Nubian"]

    def test_checkpoints_absent_degrades_to_empty(self, tmp_path):
        gtfs = StaticGtfs(build_gtfs_zip(tmp_path))
        df = gtfs.checkpoints
        assert df.empty
        assert list(df.columns) == ["checkpoint_id", "checkpoint_name"]

    def test_stop_times_exposes_checkpoint_id_when_present(self, tmp_path):
        stop_times = (
            "trip_id,arrival_time,departure_time,stop_id,stop_sequence,checkpoint_id\n"
            "T1,06:00:00,06:00:30,S1,1,HARSQ\n"
        )
        gtfs = StaticGtfs(build_gtfs_zip(tmp_path, stop_times=stop_times))
        assert gtfs.stop_times["checkpoint_id"].tolist() == ["HARSQ"]

    def test_stop_times_without_checkpoint_id_column_still_works(self, tmp_path):
        # Regression: widening usecols/dtype for checkpoint_id must not break
        # feeds (the overwhelming majority) whose stop_times.txt lacks it.
        stop_times = (
            "trip_id,arrival_time,departure_time,stop_id,stop_sequence\n"
            "T1,06:00:00,06:00:30,S1,1\n"
        )
        gtfs = StaticGtfs(build_gtfs_zip(tmp_path, stop_times=stop_times))
        assert "checkpoint_id" not in gtfs.stop_times.columns
        assert gtfs.stop_times["trip_id"].tolist() == ["T1"]


class TestEnrichEvents:
    @pytest.fixture
    def gtfs_with_schedule(self, tmp_path) -> StaticGtfs:
        calendar = (
            "service_id,monday,tuesday,wednesday,thursday,friday,saturday,sunday,start_date,end_date\n"
            "WD,1,1,1,1,1,0,0,20260501,20260531\n"
        )
        trips = "route_id,service_id,trip_id,direction_id\nR,WD,T1,0\nR,WD,T2,0\n"
        stop_times = (
            "trip_id,arrival_time,departure_time,stop_id,stop_sequence\n"
            "T1,06:00:00,06:00:30,A,1\n"
            "T1,06:05:00,06:05:30,B,2\n"
            "T2,06:10:00,06:10:30,A,1\n"
            "T2,06:15:00,06:15:30,B,2\n"
        )
        return StaticGtfs(
            build_gtfs_zip(
                tmp_path, calendar=calendar, trips=trips, stop_times=stop_times
            )
        )

    def test_populates_headway_and_tt(self, gtfs_with_schedule):
        # T2 at stop B: scheduled tt = 5 min = 300s; headway from T1 = 10 min = 600s
        events = [
            {
                "trip_id": "T2",
                "stop_id": "B",
                "scheduled_headway": "",
                "scheduled_tt": "",
            }
        ]
        gtfs_with_schedule.enrich_events(events, dt.date(2026, 5, 20))
        assert events[0]["scheduled_tt"] == 300
        assert events[0]["scheduled_headway"] == 600

    def test_early_arrival_still_populates_from_trip_id(self, gtfs_with_schedule):
        # Vehicle ran ahead of schedule — asof on event time would miss, but
        # (trip_id, stop_id) lookup is independent of actual arrival time.
        events = [
            {
                "trip_id": "T1",
                "stop_id": "B",
                "scheduled_headway": "",
                "scheduled_tt": "",
            }
        ]
        gtfs_with_schedule.enrich_events(events, dt.date(2026, 5, 20))
        # T1 is the first trip at B, so headway is undefined (empty);
        # tt is 5 min from T1's start.
        assert events[0]["scheduled_tt"] == 300
        assert events[0]["scheduled_headway"] == ""

    def test_unknown_trip_id_leaves_fields_empty(self, gtfs_with_schedule):
        events = [
            {
                "trip_id": "T_NOT_IN_GTFS",
                "stop_id": "B",
                "scheduled_headway": "",
                "scheduled_tt": "",
            }
        ]
        gtfs_with_schedule.enrich_events(events, dt.date(2026, 5, 20))
        assert events[0]["scheduled_headway"] == ""
        assert events[0]["scheduled_tt"] == ""

    def test_empty_input_is_a_noop(self, gtfs_with_schedule):
        # Just shouldn't raise
        gtfs_with_schedule.enrich_events([], dt.date(2026, 5, 20))


class TestTripDirections:
    """trip_directions powers direction_id backfill for realtime feeds that
    publish trip_id but omit direction_id (NYCT subway, TriMet, etc.)."""

    def test_basic_mapping(self, tmp_path):
        trips = (
            "trip_id,route_id,service_id,direction_id\n"
            "T1,R,WEEKDAY,0\n"
            "T2,R,WEEKDAY,1\n"
            "T3,R,WEEKDAY,0\n"
        )
        gtfs = StaticGtfs(build_gtfs_zip(tmp_path, trips=trips))
        assert gtfs.trip_directions == {
            "T1": ("R", 0),
            "T2": ("R", 1),
            "T3": ("R", 0),
        }

    def test_missing_direction_id_column_yields_none(self, tmp_path):
        # GTFS spec allows direction_id to be absent entirely.
        trips = "trip_id,route_id,service_id\nT1,R,WEEKDAY\n"
        gtfs = StaticGtfs(build_gtfs_zip(tmp_path, trips=trips))
        assert gtfs.trip_directions == {"T1": ("R", None)}

    def test_blank_direction_id_yields_none(self, tmp_path):
        # Per-row blank direction_id stays None, not 0.
        trips = (
            "trip_id,route_id,service_id,direction_id\nT1,R,WEEKDAY,0\nT2,R,WEEKDAY,\n"
        )
        gtfs = StaticGtfs(build_gtfs_zip(tmp_path, trips=trips))
        assert gtfs.trip_directions == {
            "T1": ("R", 0),
            "T2": ("R", None),
        }


class TestDirectionByTrip:
    """direction_by_trip is the thin trip_id -> direction_id projection
    segment_speed.py's compute_segment_speeds uses to override a realtime
    trip descriptor's direction_id with the static schedule's."""

    def test_basic_mapping(self, tmp_path):
        trips = (
            "trip_id,route_id,service_id,direction_id\nT1,R,WEEKDAY,0\nT2,R,WEEKDAY,1\n"
        )
        gtfs = StaticGtfs(build_gtfs_zip(tmp_path, trips=trips))
        assert gtfs.direction_by_trip == {"T1": 0, "T2": 1}

    def test_omits_trips_with_no_direction_id(self, tmp_path):
        trips = (
            "trip_id,route_id,service_id,direction_id\nT1,R,WEEKDAY,0\nT2,R,WEEKDAY,\n"
        )
        gtfs = StaticGtfs(build_gtfs_zip(tmp_path, trips=trips))
        assert gtfs.direction_by_trip == {"T1": 0}

    def test_missing_direction_id_column_yields_empty(self, tmp_path):
        trips = "trip_id,route_id,service_id\nT1,R,WEEKDAY\n"
        gtfs = StaticGtfs(build_gtfs_zip(tmp_path, trips=trips))
        assert gtfs.direction_by_trip == {}


class TestTrips:
    def test_absent_file_degrades_to_empty_frame(self, tmp_path):
        # trips.txt is required by the GTFS spec, but degrades like every
        # other table here — callers like trip_shapes/trip_directions only
        # check column presence, they don't assume the file exists.
        gtfs = StaticGtfs(build_gtfs_zip(tmp_path))  # no trips.txt at all
        df = gtfs.trips
        assert df.empty
        assert "trip_id" in df.columns
        assert "shape_id" in df.columns


class TestTripShapes:
    """trip_shapes powers shape-following distance in segment_speed.py."""

    def test_basic_mapping(self, tmp_path):
        trips = (
            "trip_id,route_id,service_id,shape_id\n"
            "T1,R,WEEKDAY,SHAPE1\n"
            "T2,R,WEEKDAY,SHAPE2\n"
        )
        gtfs = StaticGtfs(build_gtfs_zip(tmp_path, trips=trips))
        assert gtfs.trip_shapes == {"T1": "SHAPE1", "T2": "SHAPE2"}

    def test_missing_shape_id_column_yields_empty(self, tmp_path):
        # shape_id is itself GTFS-optional on trips.txt.
        trips = "trip_id,route_id,service_id\nT1,R,WEEKDAY\n"
        gtfs = StaticGtfs(build_gtfs_zip(tmp_path, trips=trips))
        assert gtfs.trip_shapes == {}

    def test_blank_shape_id_is_omitted(self, tmp_path):
        trips = (
            "trip_id,route_id,service_id,shape_id\nT1,R,WEEKDAY,SHAPE1\nT2,R,WEEKDAY,\n"
        )
        gtfs = StaticGtfs(build_gtfs_zip(tmp_path, trips=trips))
        assert gtfs.trip_shapes == {"T1": "SHAPE1"}

    def test_absent_trips_file_yields_empty(self, tmp_path):
        gtfs = StaticGtfs(build_gtfs_zip(tmp_path))
        assert gtfs.trip_shapes == {}


class TestShapePoints:
    """shape_points powers shape-following distance in segment_speed.py."""

    def test_orders_points_by_sequence(self, tmp_path):
        # Deliberately out of sequence order in the file.
        shapes = (
            "shape_id,shape_pt_lat,shape_pt_lon,shape_pt_sequence\n"
            "S1,42.36,-71.05,2\n"
            "S1,42.35,-71.06,1\n"
            "S1,42.37,-71.04,3\n"
        )
        gtfs = StaticGtfs(build_gtfs_zip(tmp_path, shapes=shapes))
        assert gtfs.shape_points["S1"] == [
            (42.35, -71.06),
            (42.36, -71.05),
            (42.37, -71.04),
        ]

    def test_separates_multiple_shapes(self, tmp_path):
        shapes = (
            "shape_id,shape_pt_lat,shape_pt_lon,shape_pt_sequence\n"
            "S1,42.35,-71.06,1\n"
            "S2,40.75,-73.98,1\n"
            "S1,42.36,-71.05,2\n"
        )
        gtfs = StaticGtfs(build_gtfs_zip(tmp_path, shapes=shapes))
        assert list(gtfs.shape_points.keys()) == ["S1", "S2"]
        assert len(gtfs.shape_points["S1"]) == 2
        assert len(gtfs.shape_points["S2"]) == 1

    def test_absent_file_yields_empty(self, tmp_path):
        gtfs = StaticGtfs(build_gtfs_zip(tmp_path))
        assert gtfs.shape_points == {}

    def test_rows_missing_lat_lon_are_dropped(self, tmp_path):
        shapes = (
            "shape_id,shape_pt_lat,shape_pt_lon,shape_pt_sequence\n"
            "S1,42.35,-71.06,1\n"
            "S1,,,2\n"
        )
        gtfs = StaticGtfs(build_gtfs_zip(tmp_path, shapes=shapes))
        assert gtfs.shape_points["S1"] == [(42.35, -71.06)]


class TestCategorizeRouteType:
    @pytest.mark.parametrize(
        "rt, expected",
        [
            (0, "rapid"),
            (1, "rapid"),
            (5, "rapid"),
            (12, "rapid"),
            (2, "cr"),
            (3, "bus"),
            (11, "bus"),
            (100, "cr"),
            (117, "cr"),
            (300, "cr"),
            (307, "cr"),
            (200, "bus"),
            (700, "bus"),
            (716, "bus"),
            (800, "bus"),
            (400, "rapid"),
            (405, "rapid"),
            (900, "rapid"),
            (906, "rapid"),
            (4, "other"),  # ferry
            (1300, "other"),  # aerial lift
            (None, "other"),
        ],
    )
    def test_known_route_types(self, rt, expected):
        assert _categorize_route_type(rt) == expected


class TestRouteModes:
    def test_mixed_modes_in_one_feed(self, tmp_path):
        # An agency like Metro Transit MN ships rail (light rail) + bus in the
        # same GTFS — route_modes must classify each route independently.
        routes = "route_id,route_type\nLRT_BLUE,0\nBUS_5,3\nCR_NORTHSTAR,2\nFERRY_X,4\n"
        gtfs = StaticGtfs(build_gtfs_zip(tmp_path, routes=routes))
        assert gtfs.route_modes == {
            "LRT_BLUE": "rapid",
            "BUS_5": "bus",
            "CR_NORTHSTAR": "cr",
            "FERRY_X": "other",
        }

    def test_missing_routes_file_yields_empty(self, tmp_path):
        # Some agency snapshots in the wild ship without routes.txt; the lookup
        # should just degrade to "no info", not raise.
        gtfs = StaticGtfs(build_gtfs_zip(tmp_path))
        assert gtfs.route_modes == {}

    def test_blank_route_type_falls_through_to_other(self, tmp_path):
        routes = "route_id,route_type\nR_OK,1\nR_BLANK,\n"
        gtfs = StaticGtfs(build_gtfs_zip(tmp_path, routes=routes))
        assert gtfs.route_modes == {"R_OK": "rapid", "R_BLANK": "other"}


class TestScoping:
    """The three multi-operator scoping modes (GO_AHEAD/Entur's agency_prefix,
    TFNSW's agency_ids and route_types) and their cascade from routes into
    trips/shapes/stop_times. Two synthetic "operators" sharing one zip, same
    shape every real case takes: AAA (bus, route_type 3) and BBB (rail,
    route_type 2)."""

    _ROUTES = "route_id,agency_id,route_type\nR1,AAA:1,3\nR2,BBB:1,2\n"
    _TRIPS = "route_id,trip_id,service_id,shape_id\nR1,T1,S1,SH1\nR2,T2,S1,SH2\n"
    _SHAPES = (
        "shape_id,shape_pt_lat,shape_pt_lon,shape_pt_sequence\n"
        "SH1,1.0,1.0,0\nSH2,2.0,2.0,0\n"
    )
    _STOP_TIMES = (
        "trip_id,stop_sequence,stop_id,arrival_time,departure_time\n"
        "T1,1,STOP1,08:00:00,08:00:00\nT2,1,STOP2,09:00:00,09:00:00\n"
    )

    def _zip(self, tmp_path):
        return build_gtfs_zip(
            tmp_path,
            routes=self._ROUTES,
            trips=self._TRIPS,
            shapes=self._SHAPES,
            stop_times=self._STOP_TIMES,
        )

    def test_no_scope_sees_both_operators(self, tmp_path):
        gtfs = StaticGtfs(self._zip(tmp_path))
        assert set(gtfs.routes["route_id"]) == {"R1", "R2"}
        assert set(gtfs.trips["trip_id"]) == {"T1", "T2"}
        assert set(gtfs.shapes["shape_id"]) == {"SH1", "SH2"}
        assert set(gtfs.stop_times["stop_id"]) == {"STOP1", "STOP2"}

    def test_agency_prefix_scopes_to_one_operator(self, tmp_path):
        gtfs = StaticGtfs(self._zip(tmp_path), agency_prefix="AAA")
        assert set(gtfs.routes["route_id"]) == {"R1"}
        assert set(gtfs.trips["trip_id"]) == {"T1"}
        assert set(gtfs.shapes["shape_id"]) == {"SH1"}
        assert set(gtfs.stop_times["stop_id"]) == {"STOP1"}

    def test_agency_ids_scopes_to_exact_set(self, tmp_path):
        gtfs = StaticGtfs(self._zip(tmp_path), agency_ids=frozenset({"BBB:1"}))
        assert set(gtfs.routes["route_id"]) == {"R2"}
        assert set(gtfs.trips["trip_id"]) == {"T2"}
        assert set(gtfs.shapes["shape_id"]) == {"SH2"}
        assert set(gtfs.stop_times["stop_id"]) == {"STOP2"}

    def test_route_types_scopes_by_mode(self, tmp_path):
        # TFNSW_BUSES' actual shape: many agency_ids, one shared route_type.
        gtfs = StaticGtfs(self._zip(tmp_path), route_types=frozenset({3}))
        assert set(gtfs.routes["route_id"]) == {"R1"}
        assert set(gtfs.trips["trip_id"]) == {"T1"}
        assert set(gtfs.shapes["shape_id"]) == {"SH1"}
        assert set(gtfs.stop_times["stop_id"]) == {"STOP1"}

    def test_route_types_matches_int_against_csv_strings(self, tmp_path):
        # route_type comes off disk via pandas' own inference, not our dtype
        # dict (unlike agency_id) -- confirms the int-vs-parsed-dtype
        # comparison in `routes` actually works, not just happens to for one
        # code path.
        gtfs = StaticGtfs(self._zip(tmp_path), route_types=frozenset({2}))
        assert set(gtfs.routes["route_id"]) == {"R2"}


# pandas import used by TestScheduledStops.test_scheduled_headway_is_per_route_dir_stop
import pandas as pd  # noqa: E402
