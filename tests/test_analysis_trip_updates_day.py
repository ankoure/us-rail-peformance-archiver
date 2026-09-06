"""Tests for analysis/trip_updates_day.py.

Builds tiny per-day trip_updates parquet files in-memory so each test can
exercise one aspect of the dedup-and-synthesize pipeline without touching
the real 100M-row metromn-trips partitions.
"""

from __future__ import annotations

import datetime as dt
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from analysis.trip_updates_day import TripUpdatesDay

FEED = "metromn-trips"
DATE = dt.date(2026, 5, 22)


def _write_parquet(base_dir: Path, rows: list[dict]) -> Path:
    """Write `rows` as the curated trip_updates parquet for (FEED, DATE)."""
    part_dir = (
        base_dir
        / "trip_updates"
        / f"feed={FEED}"
        / f"year={DATE.year}"
        / f"month={DATE.month}"
        / f"day={DATE.day}"
    )
    part_dir.mkdir(parents=True, exist_ok=True)
    # Build columns matching the curated schema (dotted protobuf paths, per
    # StopTimeUpdateRow's TableSpec.column_names in archiver/decoder.py).
    schema_cols = [
        "trip_update.trip.trip_id",
        "trip_update.trip.route_id",
        "trip_update.trip.direction_id",
        "trip_update.stop_time_update.stop_id",
        "trip_update.stop_time_update.stop_sequence",
        "trip_update.stop_time_update.arrival.time",
        "trip_update.stop_time_update.departure.time",
        "feed_timestamp",
        "trip_update.stop_time_update.schedule_relationship",
        "trip_update.vehicle.id",
        "trip_update.vehicle.label",
    ]
    cols: dict[str, list] = {c: [r.get(c) for r in rows] for c in schema_cols}
    table = pa.table(cols)
    pq.write_table(table, part_dir / "data.parquet")
    return part_dir


def _write_parquet_multi_group(base_dir: Path, row_groups: list[list[dict]]) -> Path:
    """Like _write_parquet, but each inner list becomes its OWN row group --
    for exercising the cross-row-group merge path directly (a single
    pq.write_table call always produces one row group for data this small)."""
    part_dir = (
        base_dir
        / "trip_updates"
        / f"feed={FEED}"
        / f"year={DATE.year}"
        / f"month={DATE.month}"
        / f"day={DATE.day}"
    )
    part_dir.mkdir(parents=True, exist_ok=True)
    schema_cols = [
        "trip_update.trip.trip_id",
        "trip_update.trip.route_id",
        "trip_update.trip.direction_id",
        "trip_update.stop_time_update.stop_id",
        "trip_update.stop_time_update.stop_sequence",
        "trip_update.stop_time_update.arrival.time",
        "trip_update.stop_time_update.departure.time",
        "feed_timestamp",
        "trip_update.stop_time_update.schedule_relationship",
        "trip_update.vehicle.id",
        "trip_update.vehicle.label",
    ]
    path = part_dir / "data.parquet"
    with pq.ParquetWriter(
        path,
        pa.schema(
            [
                (c, pa.string())
                if c
                not in {
                    "trip_update.trip.direction_id",
                    "trip_update.stop_time_update.stop_sequence",
                    "trip_update.stop_time_update.arrival.time",
                    "trip_update.stop_time_update.departure.time",
                    "feed_timestamp",
                }
                else (c, pa.int64())
                for c in schema_cols
            ]
        ),
    ) as writer:
        for rows in row_groups:
            cols = {c: [r.get(c) for r in rows] for c in schema_cols}
            writer.write_table(pa.table(cols, schema=writer.schema))
    return part_dir


def _row(
    *,
    trip_id: str = "T1",
    route_id: str = "901",
    direction_id: int | None = 0,
    stop_id: str = "S1",
    stop_sequence: int | None = 1,
    arrival_time: int | None = None,
    departure_time: int | None = 1_700_000_000,
    feed_timestamp: int = 1_700_000_000,
    sched_rel: str | None = "SCHEDULED",
    vehicle_id: str | None = "V1",
    vehicle_label: str | None = None,
) -> dict:
    return {
        "trip_update.trip.trip_id": trip_id,
        "trip_update.trip.route_id": route_id,
        "trip_update.trip.direction_id": direction_id,
        "trip_update.stop_time_update.stop_id": stop_id,
        "trip_update.stop_time_update.stop_sequence": stop_sequence,
        "trip_update.stop_time_update.arrival.time": arrival_time,
        "trip_update.stop_time_update.departure.time": departure_time,
        "feed_timestamp": feed_timestamp,
        "trip_update.stop_time_update.schedule_relationship": sched_rel,
        "trip_update.vehicle.id": vehicle_id,
        "trip_update.vehicle.label": vehicle_label,
    }


class TestLatestPredictionWins:
    def test_keeps_max_feed_timestamp_per_trip_stop(self, tmp_path):
        # Three predictions for the same (trip, stop), feed_timestamp grows.
        # The final-published departure_time (1005) should be the recorded one.
        rows = [
            _row(feed_timestamp=1000, departure_time=995),
            _row(feed_timestamp=1010, departure_time=1003),
            _row(feed_timestamp=1020, departure_time=1005),
        ]
        _write_parquet(tmp_path, rows)
        day = TripUpdatesDay(FEED, DATE, base_dir=tmp_path)
        assert len(day.visits) == 1
        v = day.visits[0]
        assert v.departure_ts == 1005

    def test_out_of_order_polls_still_pick_max(self, tmp_path):
        rows = [
            _row(feed_timestamp=1020, departure_time=1005),
            _row(feed_timestamp=1000, departure_time=995),  # stale, ignored
            _row(feed_timestamp=1010, departure_time=1003),
        ]
        _write_parquet(tmp_path, rows)
        day = TripUpdatesDay(FEED, DATE, base_dir=tmp_path)
        assert day.visits[0].departure_ts == 1005


class TestArrivalFallback:
    def test_arrival_missing_falls_back_to_departure(self, tmp_path):
        # LRT case — agencies (e.g. metromn) publish only departure_time.
        rows = [_row(arrival_time=None, departure_time=1_700_000_500)]
        _write_parquet(tmp_path, rows)
        day = TripUpdatesDay(FEED, DATE, base_dir=tmp_path)
        v = day.visits[0]
        assert v.arrival_ts == v.departure_ts == 1_700_000_500

    def test_departure_missing_falls_back_to_arrival(self, tmp_path):
        # Symmetric case — some feeds publish arrival but not departure.
        rows = [_row(arrival_time=1_700_000_400, departure_time=None)]
        _write_parquet(tmp_path, rows)
        day = TripUpdatesDay(FEED, DATE, base_dir=tmp_path)
        v = day.visits[0]
        assert v.arrival_ts == v.departure_ts == 1_700_000_400

    def test_both_missing_drops_row(self, tmp_path):
        rows = [_row(arrival_time=None, departure_time=None)]
        _write_parquet(tmp_path, rows)
        day = TripUpdatesDay(FEED, DATE, base_dir=tmp_path)
        assert day.visits == []


class TestSchedRelationshipFilter:
    def test_skipped_stop_is_dropped(self, tmp_path):
        rows = [
            _row(trip_id="T1", stop_id="S1", sched_rel="SCHEDULED"),
            _row(trip_id="T1", stop_id="S2", sched_rel="SKIPPED"),
            _row(trip_id="T1", stop_id="S3", sched_rel="NO_DATA"),
        ]
        _write_parquet(tmp_path, rows)
        day = TripUpdatesDay(FEED, DATE, base_dir=tmp_path)
        assert {v.stop_id for v in day.visits} == {"S1"}

    def test_null_sched_rel_treated_as_real(self, tmp_path):
        # Spec says missing schedule_relationship implies SCHEDULED.
        rows = [_row(stop_id="S1", sched_rel=None)]
        _write_parquet(tmp_path, rows)
        day = TripUpdatesDay(FEED, DATE, base_dir=tmp_path)
        assert len(day.visits) == 1


class TestVisitFields:
    def test_carries_trip_route_direction_vehicle(self, tmp_path):
        rows = [
            _row(
                trip_id="T_X",
                route_id="901",
                direction_id=1,
                stop_id="S9",
                stop_sequence=7,
                vehicle_id="V_42",
                departure_time=1_700_000_777,
            )
        ]
        _write_parquet(tmp_path, rows)
        day = TripUpdatesDay(FEED, DATE, base_dir=tmp_path)
        v = day.visits[0]
        assert v.trip_id == "T_X"
        assert v.route_id == "901"
        assert v.direction_id == 1
        assert v.stop_id == "S9"
        assert v.stop_sequence == 7
        assert v.vehicle_id == "V_42"
        assert v.departure_ts == 1_700_000_777

    def test_falls_back_to_vehicle_label_when_id_null(self, tmp_path):
        rows = [_row(vehicle_id=None, vehicle_label="LBL_99")]
        _write_parquet(tmp_path, rows)
        day = TripUpdatesDay(FEED, DATE, base_dir=tmp_path)
        assert day.visits[0].vehicle_id == "LBL_99"


class TestVehiclesGrouping:
    def test_visits_grouped_by_vehicle_id(self, tmp_path):
        # Three predictions across two vehicles → exporter sees 2 stub vehicles.
        rows = [
            _row(trip_id="T1", stop_id="S1", vehicle_id="V1"),
            _row(trip_id="T1", stop_id="S2", vehicle_id="V1"),
            _row(trip_id="T2", stop_id="S1", vehicle_id="V2"),
        ]
        _write_parquet(tmp_path, rows)
        day = TripUpdatesDay(FEED, DATE, base_dir=tmp_path)
        by_vid = {v.vehicle_id: v.dwells for v in day.vehicles}
        assert set(by_vid) == {"V1", "V2"}
        assert len(by_vid["V1"]) == 2
        assert len(by_vid["V2"]) == 1


class TestCrossRowGroupMerge:
    def test_later_row_group_wins_same_key(self, tmp_path):
        """Same (trip, stop) key split across two SEPARATE row groups --
        exercises the incremental per-key merge directly (2026-09-06 rewrite
        replaced a single global concat+dedupe with folding each row group's
        already-deduped winner into a running dict; this is the case that
        distinguishes the two: a key whose winning row is in a DIFFERENT
        row group than where it's first seen)."""
        _write_parquet_multi_group(
            tmp_path,
            [
                [_row(feed_timestamp=1000, departure_time=995)],
                [_row(feed_timestamp=2000, departure_time=1998)],
                [_row(feed_timestamp=1500, departure_time=1495)],  # older, mid-stream
            ],
        )
        day = TripUpdatesDay(FEED, DATE, base_dir=tmp_path)
        assert len(day.visits) == 1
        assert day.visits[0].departure_ts == 1998

    def test_same_key_wins_across_separate_merge_batches(self, tmp_path, monkeypatch):
        """Same as above, but small enough real data (a handful of rows)
        would normally all land in ONE batch before the first flush --
        forcing _MERGE_BATCH_ROWS down to 1 row makes every row group its own
        batch, so this actually exercises the `merged is not None` branch of
        `visits`'s flush() (merging the running per-key-winner table against
        a NEW batch's dedupe result), not just one batch's own internal
        concat+dedupe."""
        import analysis.trip_updates_day as tud

        monkeypatch.setattr(tud, "_MERGE_BATCH_ROWS", 1)
        _write_parquet_multi_group(
            tmp_path,
            [
                [_row(feed_timestamp=1000, departure_time=995)],
                [_row(feed_timestamp=2000, departure_time=1998)],
                [_row(feed_timestamp=1500, departure_time=1495)],  # older, mid-stream
            ],
        )
        day = TripUpdatesDay(FEED, DATE, base_dir=tmp_path)
        assert len(day.visits) == 1
        assert day.visits[0].departure_ts == 1998

    def test_distinct_keys_across_row_groups_all_survive(self, tmp_path):
        _write_parquet_multi_group(
            tmp_path,
            [
                [_row(trip_id="T1", stop_id="S1")],
                [_row(trip_id="T2", stop_id="S2")],
                [_row(trip_id="T3", stop_id="S3")],
            ],
        )
        day = TripUpdatesDay(FEED, DATE, base_dir=tmp_path)
        assert {(v.trip_id, v.stop_id) for v in day.visits} == {
            ("T1", "S1"),
            ("T2", "S2"),
            ("T3", "S3"),
        }


class TestMissingPartition:
    def test_raises_filenotfound(self, tmp_path):
        day = TripUpdatesDay(FEED, DATE, base_dir=tmp_path)
        with pytest.raises(FileNotFoundError):
            _ = day.visits
