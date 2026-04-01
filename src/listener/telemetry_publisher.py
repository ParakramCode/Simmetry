"""
Telemetry Publisher — Week 1: CSV/JSON writer, Week 2: Kafka producer.

In Week 1, this writes telemetry data to local CSV files for validation.
In Week 2, it will be refactored to publish to Kafka topics instead.

File output structure (Week 1):
  data/raw/{session_id}/
    ├── telemetry.csv           # 60Hz telemetry data
    ├── session_events.jsonl    # Session start/end events
    ├── lap_completions.jsonl   # One line per completed lap
    └── setup_snapshots.jsonl   # Setup state at changes
"""

import csv
import json
import os
from dataclasses import asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional, TextIO, Dict
from loguru import logger

from .acc_reader import ACCTelemetrySnapshot
from .session_manager import SessionEvent, LapCompletionEvent, SetupChangeEvent, SetupSnapshot
from config.settings import RAW_DATA_DIR


# ═══════════════════════════════════════════════════════════════
# CSV Telemetry Columns (flat representation for Parquet compatibility)
# ═══════════════════════════════════════════════════════════════

TELEMETRY_COLUMNS = [
    # Identity / timing
    "timestamp_wall", "timestamp_game",
    "car", "track", "session_type", "status",
    
    # Lap info
    "completed_laps", "current_lap_time_ms", "last_lap_time_ms",
    "best_lap_time_ms", "distance_into_lap", "normalized_position",
    "current_sector", "last_sector_time_ms", "is_valid_lap",
    "is_in_pit", "is_in_pit_lane",
    
    # Inputs
    "throttle", "brake", "steering", "clutch",
    
    # Dynamics
    "speed_kmh", "gear", "rpm", "fuel_remaining", "fuel_per_lap",
    
    # G-forces
    "g_lat", "g_lon", "g_vert",
    
    # Tyres — flattened from WheelData
    "tyre_pressure_fl", "tyre_pressure_fr", "tyre_pressure_rl", "tyre_pressure_rr",
    "tyre_temp_fl", "tyre_temp_fr", "tyre_temp_rl", "tyre_temp_rr",
    "tyre_inner_temp_fl", "tyre_inner_temp_fr", "tyre_inner_temp_rl", "tyre_inner_temp_rr",
    "tyre_middle_temp_fl", "tyre_middle_temp_fr", "tyre_middle_temp_rl", "tyre_middle_temp_rr",
    "tyre_outer_temp_fl", "tyre_outer_temp_fr", "tyre_outer_temp_rl", "tyre_outer_temp_rr",
    "tyre_slip_fl", "tyre_slip_fr", "tyre_slip_rl", "tyre_slip_rr",
    "suspension_travel_fl", "suspension_travel_fr", "suspension_travel_rl", "suspension_travel_rr",
    "brake_temp_fl", "brake_temp_fr", "brake_temp_rl", "brake_temp_rr",
    "tyre_wear_fl", "tyre_wear_fr", "tyre_wear_rl", "tyre_wear_rr",
    
    # Car settings
    "tc_level", "tc_cut", "abs_level", "engine_map", "brake_bias",
    
    # Environment
    "air_temp", "road_temp", "track_grip", "rain_intensity",
    "wind_speed", "wind_direction",
]


class CSVTelemetryPublisher:
    """
    Week 1 publisher — writes telemetry to local CSV/JSONL files.
    
    Creates a directory per session under data/raw/ and writes:
      - telemetry.csv: 60Hz telemetry readings (one row per snapshot)
      - session_events.jsonl: Session start/end events
      - lap_completions.jsonl: Lap completion events
      - setup_snapshots.jsonl: Setup state at change points
    """
    
    def __init__(self):
        self._session_dir: Optional[Path] = None
        self._csv_file: Optional[TextIO] = None
        self._csv_writer = None
        self._event_files: Dict[str, TextIO] = {}
        self._row_count = 0
        
        logger.info("CSVTelemetryPublisher initialized")
    
    def start_session(self, session_id: str):
        """Open files for a new recording session."""
        self._session_dir = RAW_DATA_DIR / session_id
        self._session_dir.mkdir(parents=True, exist_ok=True)
        
        # Open telemetry CSV
        csv_path = self._session_dir / "telemetry.csv"
        self._csv_file = open(csv_path, "w", newline="", encoding="utf-8")
        self._csv_writer = csv.DictWriter(self._csv_file, fieldnames=TELEMETRY_COLUMNS)
        self._csv_writer.writeheader()
        self._row_count = 0
        
        # Open event JSONL files
        for name in ["session_events", "lap_completions", "setup_snapshots"]:
            path = self._session_dir / f"{name}.jsonl"
            self._event_files[name] = open(path, "w", encoding="utf-8")
        
        logger.info(f"📁 Recording to: {self._session_dir}")
    
    def write_telemetry(self, snapshot: ACCTelemetrySnapshot):
        """Write a single telemetry snapshot to CSV."""
        if not self._csv_writer:
            return
        
        row = self._flatten_snapshot(snapshot)
        self._csv_writer.writerow(row)
        self._row_count += 1
        
        # Flush every 600 rows (~10 seconds at 60Hz) for crash safety
        if self._row_count % 600 == 0:
            self._csv_file.flush()
            logger.debug(f"  Written {self._row_count} telemetry rows")
    
    def write_session_event(self, event: SessionEvent):
        """Write a session event to JSONL."""
        f = self._event_files.get("session_events")
        if f:
            f.write(json.dumps(event.to_dict()) + "\n")
            f.flush()
    
    def write_lap_completion(self, event: LapCompletionEvent):
        """Write a lap completion event to JSONL."""
        f = self._event_files.get("lap_completions")
        if f:
            f.write(json.dumps(event.to_dict()) + "\n")
            f.flush()
    
    def write_setup_snapshot(self, event: SetupSnapshot):
        """Write a setup snapshot to JSONL."""
        f = self._event_files.get("setup_snapshots")
        if f:
            f.write(json.dumps(event.to_dict()) + "\n")
            f.flush()
    
    def end_session(self):
        """Close all files for the current session."""
        if self._csv_file:
            self._csv_file.close()
            self._csv_file = None
            self._csv_writer = None
        
        for f in self._event_files.values():
            f.close()
        self._event_files.clear()
        
        logger.info(f"📁 Session recording complete. Total rows: {self._row_count}")
        self._row_count = 0
    
    def _flatten_snapshot(self, s: ACCTelemetrySnapshot) -> dict:
        """Flatten an ACCTelemetrySnapshot into a CSV row dict."""
        return {
            "timestamp_wall": s.timestamp_wall.isoformat(),
            "timestamp_game": f"{s.timestamp_game:.3f}",
            "car": s.car,
            "track": s.track,
            "session_type": s.session_type,
            "status": s.status,
            
            "completed_laps": s.completed_laps,
            "current_lap_time_ms": s.current_lap_time_ms,
            "last_lap_time_ms": s.last_lap_time_ms,
            "best_lap_time_ms": s.best_lap_time_ms,
            "distance_into_lap": f"{s.distance_into_lap:.2f}",
            "normalized_position": f"{s.normalized_position:.6f}",
            "current_sector": s.current_sector,
            "last_sector_time_ms": s.last_sector_time_ms,
            "is_valid_lap": s.is_valid_lap,
            "is_in_pit": s.is_in_pit,
            "is_in_pit_lane": s.is_in_pit_lane,
            
            "throttle": f"{s.throttle:.4f}",
            "brake": f"{s.brake:.4f}",
            "steering": f"{s.steering:.4f}",
            "clutch": f"{s.clutch:.4f}",
            
            "speed_kmh": f"{s.speed_kmh:.2f}",
            "gear": s.gear,
            "rpm": s.rpm,
            "fuel_remaining": f"{s.fuel_remaining:.2f}",
            "fuel_per_lap": f"{s.fuel_per_lap:.2f}",
            
            "g_lat": f"{s.g_lat:.4f}",
            "g_lon": f"{s.g_lon:.4f}",
            "g_vert": f"{s.g_vert:.4f}",
            
            # Tyre data — flattened per corner
            "tyre_pressure_fl": f"{s.wheel_fl.pressure:.2f}",
            "tyre_pressure_fr": f"{s.wheel_fr.pressure:.2f}",
            "tyre_pressure_rl": f"{s.wheel_rl.pressure:.2f}",
            "tyre_pressure_rr": f"{s.wheel_rr.pressure:.2f}",
            "tyre_temp_fl": f"{s.wheel_fl.core_temp:.1f}",
            "tyre_temp_fr": f"{s.wheel_fr.core_temp:.1f}",
            "tyre_temp_rl": f"{s.wheel_rl.core_temp:.1f}",
            "tyre_temp_rr": f"{s.wheel_rr.core_temp:.1f}",
            "tyre_inner_temp_fl": f"{s.wheel_fl.inner_temp:.1f}",
            "tyre_inner_temp_fr": f"{s.wheel_fr.inner_temp:.1f}",
            "tyre_inner_temp_rl": f"{s.wheel_rl.inner_temp:.1f}",
            "tyre_inner_temp_rr": f"{s.wheel_rr.inner_temp:.1f}",
            "tyre_middle_temp_fl": f"{s.wheel_fl.middle_temp:.1f}",
            "tyre_middle_temp_fr": f"{s.wheel_fr.middle_temp:.1f}",
            "tyre_middle_temp_rl": f"{s.wheel_rl.middle_temp:.1f}",
            "tyre_middle_temp_rr": f"{s.wheel_rr.middle_temp:.1f}",
            "tyre_outer_temp_fl": f"{s.wheel_fl.outer_temp:.1f}",
            "tyre_outer_temp_fr": f"{s.wheel_fr.outer_temp:.1f}",
            "tyre_outer_temp_rl": f"{s.wheel_rl.outer_temp:.1f}",
            "tyre_outer_temp_rr": f"{s.wheel_rr.outer_temp:.1f}",
            "tyre_slip_fl": f"{s.wheel_fl.slip:.4f}",
            "tyre_slip_fr": f"{s.wheel_fr.slip:.4f}",
            "tyre_slip_rl": f"{s.wheel_rl.slip:.4f}",
            "tyre_slip_rr": f"{s.wheel_rr.slip:.4f}",
            "suspension_travel_fl": f"{s.wheel_fl.suspension_travel:.4f}",
            "suspension_travel_fr": f"{s.wheel_fr.suspension_travel:.4f}",
            "suspension_travel_rl": f"{s.wheel_rl.suspension_travel:.4f}",
            "suspension_travel_rr": f"{s.wheel_rr.suspension_travel:.4f}",
            "brake_temp_fl": f"{s.wheel_fl.brake_temp:.1f}",
            "brake_temp_fr": f"{s.wheel_fr.brake_temp:.1f}",
            "brake_temp_rl": f"{s.wheel_rl.brake_temp:.1f}",
            "brake_temp_rr": f"{s.wheel_rr.brake_temp:.1f}",
            "tyre_wear_fl": f"{s.wheel_fl.wear:.4f}",
            "tyre_wear_fr": f"{s.wheel_fr.wear:.4f}",
            "tyre_wear_rl": f"{s.wheel_rl.wear:.4f}",
            "tyre_wear_rr": f"{s.wheel_rr.wear:.4f}",
            
            "tc_level": s.tc_level,
            "tc_cut": s.tc_cut,
            "abs_level": s.abs_level,
            "engine_map": s.engine_map,
            "brake_bias": f"{s.brake_bias:.1f}",
            
            "air_temp": f"{s.air_temp:.1f}",
            "road_temp": f"{s.road_temp:.1f}",
            "track_grip": s.track_grip,
            "rain_intensity": s.rain_intensity,
            "wind_speed": f"{s.wind_speed:.1f}",
            "wind_direction": f"{s.wind_direction:.1f}",
        }
    
    def close(self):
        """Ensure all files are closed."""
        self.end_session()
