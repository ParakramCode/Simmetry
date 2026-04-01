"""
Lap Segmenter — Splits continuous telemetry stream into per-lap Parquet files.

Using normalizedCarPosition + completedLaps to detect lap boundaries:
  - completedLaps increment → new lap started
  - normalizedCarPosition resets to ~0 → secondary confirmation

Each detected lap gets written as a standalone Parquet file using the
project's path convention:
  telemetry/{platform}/{track}/{car}/{session_id}/lap_{NNN}.parquet

This enables DuckDB to read individual laps or glob entire sessions
without loading the full 30Hz/60Hz CSV into memory.
"""

from collections import deque
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional, List, Dict
from loguru import logger

import pyarrow as pa
import pyarrow.parquet as pq

from .acc_reader import ACCTelemetrySnapshot
from config.settings import get_parquet_path


# ═══════════════════════════════════════════════════════════════
# Arrow Schema — defines the Parquet column types
# ═══════════════════════════════════════════════════════════════

TELEMETRY_SCHEMA = pa.schema([
    # Identity / timing
    ("timestamp_wall", pa.timestamp("ms", tz="UTC")),
    ("timestamp_game", pa.float32()),
    ("session_id", pa.string()),
    ("car", pa.string()),
    ("track", pa.string()),
    ("session_type", pa.string()),
    ("lap_number", pa.int32()),

    # Lap / position
    ("distance_into_lap", pa.float32()),
    ("normalized_position", pa.float32()),
    ("current_sector", pa.int8()),
    ("is_valid_lap", pa.bool_()),

    # Inputs
    ("throttle", pa.float32()),
    ("brake", pa.float32()),
    ("steering", pa.float32()),
    ("clutch", pa.float32()),

    # Dynamics
    ("speed_kmh", pa.float32()),
    ("gear", pa.int8()),
    ("rpm", pa.int32()),
    ("fuel_remaining", pa.float32()),

    # G-forces
    ("g_lat", pa.float32()),
    ("g_lon", pa.float32()),
    ("g_vert", pa.float32()),

    # Tyre core temps (from tyreTemp — per SDK)
    ("tyre_temp_fl", pa.float32()),
    ("tyre_temp_fr", pa.float32()),
    ("tyre_temp_rl", pa.float32()),
    ("tyre_temp_rr", pa.float32()),

    # Tyre pressures
    ("tyre_pressure_fl", pa.float32()),
    ("tyre_pressure_fr", pa.float32()),
    ("tyre_pressure_rl", pa.float32()),
    ("tyre_pressure_rr", pa.float32()),

    # Tyre wear
    ("tyre_wear_fl", pa.float32()),
    ("tyre_wear_fr", pa.float32()),
    ("tyre_wear_rl", pa.float32()),
    ("tyre_wear_rr", pa.float32()),

    # Tyre slip
    ("tyre_slip_fl", pa.float32()),
    ("tyre_slip_fr", pa.float32()),
    ("tyre_slip_rl", pa.float32()),
    ("tyre_slip_rr", pa.float32()),

    # Brake temps
    ("brake_temp_fl", pa.float32()),
    ("brake_temp_fr", pa.float32()),
    ("brake_temp_rl", pa.float32()),
    ("brake_temp_rr", pa.float32()),

    # Suspension
    ("suspension_travel_fl", pa.float32()),
    ("suspension_travel_fr", pa.float32()),
    ("suspension_travel_rl", pa.float32()),
    ("suspension_travel_rr", pa.float32()),

    # Car settings
    ("tc_level", pa.int8()),
    ("tc_cut", pa.int8()),
    ("abs_level", pa.int8()),
    ("engine_map", pa.int8()),
    ("brake_bias", pa.float32()),

    # Environment
    ("air_temp", pa.float32()),
    ("road_temp", pa.float32()),
    ("track_grip", pa.string()),
    ("rain_intensity", pa.string()),
])


class LapSegmenter:
    """
    Accumulates telemetry samples and writes per-lap Parquet files.

    Usage:
        segmenter = LapSegmenter(session_id="acc_20260330_...", platform="ACC")
        
        for snapshot in telemetry_stream:
            segmenter.add_sample(snapshot)
        
        segmenter.flush()  # Write any remaining data
    """

    def __init__(self, session_id: str, platform: str = "ACC"):
        self._session_id = session_id
        self._platform = platform

        # Buffer for current lap's samples
        self._current_lap_buffer: List[dict] = []
        self._current_lap_number: int = 0
        self._laps_written: int = 0

        # Track state for boundary detection
        self._last_completed_laps: int = -1

        logger.info(f"LapSegmenter initialized for session {session_id}")

    def add_sample(self, snapshot: ACCTelemetrySnapshot):
        """
        Add a telemetry sample. Detects lap boundaries and flushes
        completed laps to Parquet.
        """
        # Detect lap boundary via completedLaps increment
        if self._last_completed_laps >= 0 and snapshot.completed_laps > self._last_completed_laps:
            # Lap boundary crossed — write the buffer as the previous lap
            if self._current_lap_buffer:
                self._write_lap(
                    lap_number=self._last_completed_laps,
                    samples=self._current_lap_buffer,
                    car=snapshot.car,
                    track=snapshot.track,
                )
                self._current_lap_buffer = []

            self._current_lap_number = snapshot.completed_laps

        self._last_completed_laps = snapshot.completed_laps

        # Skip samples while in pit
        if snapshot.is_in_pit:
            return

        # Flatten snapshot into a row dict
        row = self._flatten(snapshot)
        self._current_lap_buffer.append(row)

    def _flatten(self, s: ACCTelemetrySnapshot) -> dict:
        """Flatten an ACCTelemetrySnapshot into a Parquet row."""
        return {
            "timestamp_wall": s.timestamp_wall,
            "timestamp_game": s.timestamp_game,
            "session_id": self._session_id,
            "car": s.car,
            "track": s.track,
            "session_type": s.session_type,
            "lap_number": s.completed_laps,

            "distance_into_lap": s.distance_into_lap,
            "normalized_position": s.normalized_position,
            "current_sector": s.current_sector,
            "is_valid_lap": s.is_valid_lap,

            "throttle": s.throttle,
            "brake": s.brake,
            "steering": s.steering,
            "clutch": s.clutch,

            "speed_kmh": s.speed_kmh,
            "gear": s.gear,
            "rpm": s.rpm,
            "fuel_remaining": s.fuel_remaining,

            "g_lat": s.g_lat,
            "g_lon": s.g_lon,
            "g_vert": s.g_vert,

            "tyre_temp_fl": s.wheel_fl.core_temp,
            "tyre_temp_fr": s.wheel_fr.core_temp,
            "tyre_temp_rl": s.wheel_rl.core_temp,
            "tyre_temp_rr": s.wheel_rr.core_temp,

            "tyre_pressure_fl": s.wheel_fl.pressure,
            "tyre_pressure_fr": s.wheel_fr.pressure,
            "tyre_pressure_rl": s.wheel_rl.pressure,
            "tyre_pressure_rr": s.wheel_rr.pressure,

            "tyre_wear_fl": s.wheel_fl.wear,
            "tyre_wear_fr": s.wheel_fr.wear,
            "tyre_wear_rl": s.wheel_rl.wear,
            "tyre_wear_rr": s.wheel_rr.wear,

            "tyre_slip_fl": s.wheel_fl.slip,
            "tyre_slip_fr": s.wheel_fr.slip,
            "tyre_slip_rl": s.wheel_rl.slip,
            "tyre_slip_rr": s.wheel_rr.slip,

            "brake_temp_fl": s.wheel_fl.brake_temp,
            "brake_temp_fr": s.wheel_fr.brake_temp,
            "brake_temp_rl": s.wheel_rl.brake_temp,
            "brake_temp_rr": s.wheel_rr.brake_temp,

            "suspension_travel_fl": s.wheel_fl.suspension_travel,
            "suspension_travel_fr": s.wheel_fr.suspension_travel,
            "suspension_travel_rl": s.wheel_rl.suspension_travel,
            "suspension_travel_rr": s.wheel_rr.suspension_travel,

            "tc_level": s.tc_level,
            "tc_cut": s.tc_cut,
            "abs_level": s.abs_level,
            "engine_map": s.engine_map,
            "brake_bias": s.brake_bias,

            "air_temp": s.air_temp,
            "road_temp": s.road_temp,
            "track_grip": s.track_grip,
            "rain_intensity": s.rain_intensity,
        }

    def _write_lap(self, lap_number: int, samples: List[dict], car: str, track: str):
        """Write a completed lap's samples to a Parquet file."""
        if not samples:
            return

        path = get_parquet_path(
            platform=self._platform,
            track=track,
            car=car,
            session_id=self._session_id,
            lap_number=lap_number,
        )

        # Build columnar arrays from row dicts
        columns = {col: [] for col in TELEMETRY_SCHEMA.names}
        for row in samples:
            for col in TELEMETRY_SCHEMA.names:
                columns[col].append(row.get(col))

        table = pa.table(columns, schema=TELEMETRY_SCHEMA)
        pq.write_table(table, str(path), compression="snappy")

        self._laps_written += 1
        size_kb = path.stat().st_size / 1024

        logger.info(
            f"  Wrote lap {lap_number} -> {path.name} "
            f"({len(samples)} samples, {size_kb:.1f} KB)"
        )

    def flush(self):
        """Write any remaining buffered data (for incomplete laps)."""
        if self._current_lap_buffer:
            # Use the last sample to get car/track info
            last = self._current_lap_buffer[-1]
            self._write_lap(
                lap_number=self._current_lap_number,
                samples=self._current_lap_buffer,
                car=last.get("car", "unknown"),
                track=last.get("track", "unknown"),
            )
            self._current_lap_buffer = []

    @property
    def laps_written(self) -> int:
        return self._laps_written

    def close(self):
        """Flush and clean up."""
        self.flush()
        logger.info(
            f"LapSegmenter closed. Total laps written: {self._laps_written}"
        )
