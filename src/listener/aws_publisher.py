"""
AWS Telemetry Publisher — Cloud Migration

Replaces KafkaTelemetryPublisher for AWS deployment mode.
Sends telemetry data to:
  1. Kinesis Data Firehose → S3 (raw JSON, auto-batched)
  2. S3 directly for per-lap Parquet files (replaces local LapSegmenter)
  3. S3 for session/lap/setup event JSONL files

This publisher maintains the same interface as KafkaTelemetryPublisher
so the main.py listener loop requires minimal changes.
"""

import io
import json
from datetime import datetime, timezone
from typing import Optional, Dict, Any, List
from loguru import logger

import boto3
import pyarrow as pa
import pyarrow.parquet as pq

from .acc_reader import ACCTelemetrySnapshot
from .session_manager import SessionEvent, LapCompletionEvent, SetupChangeEvent, SetupSnapshot
from .lap_segmenter import TELEMETRY_SCHEMA  # Reuse the same Arrow schema
from config.settings import (
    AWS_REGION,
    S3_BUCKET,
    S3_RAW_PREFIX,
    S3_PROCESSED_PREFIX,
    FIREHOSE_STREAM_NAME,
    get_s3_parquet_key,
)


class AWSPublisher:
    """
    Publishes ACCTelemetrySnapshot and session events to AWS.

    - Raw 30Hz telemetry → Kinesis Data Firehose → S3 (auto-batched JSON)
    - Per-lap Parquet files → S3 directly
    - Session/Lap/Setup events → S3 as JSONL files
    """

    def __init__(self):
        self._firehose = boto3.client("firehose", region_name=AWS_REGION)
        self._s3 = boto3.client("s3", region_name=AWS_REGION)

        self._current_session_id: Optional[str] = None
        self._row_count = 0
        self._firehose_failures = 0

        # Firehose batching buffer (put_record_batch supports up to 500 records / 4MB)
        self._firehose_buffer: List[bytes] = []
        self._firehose_buffer_bytes = 0
        self._FIREHOSE_BATCH_SIZE = 400        # records per batch (max 500)
        self._FIREHOSE_BATCH_BYTES = 3_500_000  # bytes per batch (max 4MB)

        # Per-lap Parquet buffer (same approach as LapSegmenter)
        self._current_lap_buffer: List[dict] = []
        self._last_completed_laps: int = -1
        self._laps_written: int = 0

        # Event accumulation for S3 JSONL
        self._session_events: List[str] = []
        self._lap_events: List[str] = []
        self._setup_events: List[str] = []

        # Local fallback buffer for network resilience
        self._local_buffer: List[dict] = []
        self._MAX_LOCAL_BUFFER = 18000  # ~10 minutes at 30Hz

        logger.info(
            f"AWSPublisher initialized | Region: {AWS_REGION} | "
            f"Bucket: {S3_BUCKET} | Firehose: {FIREHOSE_STREAM_NAME}"
        )

    # ── Session Lifecycle ──

    def start_session(self, session_id: str):
        """Initialize publisher for a new session."""
        self._current_session_id = session_id
        self._row_count = 0
        self._firehose_failures = 0
        self._current_lap_buffer = []
        self._last_completed_laps = -1
        self._laps_written = 0
        self._session_events = []
        self._lap_events = []
        self._setup_events = []
        logger.info(f"☁️  Streaming to AWS for session: {session_id}")

    def end_session(self):
        """Finish session: flush Firehose buffer, write remaining Parquet, upload event files."""
        if not self._current_session_id:
            return

        logger.info(f"☁️  Session {self._current_session_id} ending. Flushing to AWS...")

        # 1. Flush any remaining Firehose records
        self._flush_firehose_buffer()

        # 2. Flush any remaining lap data as Parquet to S3
        self._flush_current_lap()

        # 3. Upload accumulated event JSONL files to S3
        self._upload_events_to_s3()

        logger.info(
            f"☁️  Session complete | {self._row_count} telemetry rows | "
            f"{self._laps_written} lap Parquets | "
            f"Firehose failures: {self._firehose_failures}"
        )

        self._current_session_id = None
        self._row_count = 0

    # ── Telemetry ──

    def write_telemetry(self, snapshot: ACCTelemetrySnapshot):
        """Publish a single telemetry snapshot to Firehose + buffer for Parquet."""
        if not self._current_session_id:
            return

        data = self._flatten_snapshot(snapshot)
        data["session_id"] = self._current_session_id

        # 1. Send to Firehose (raw ingestion layer)
        json_line = json.dumps(data) + "\n"
        self._add_to_firehose_buffer(json_line.encode("utf-8"))

        # 2. Buffer for per-lap Parquet (same logic as LapSegmenter)
        if self._last_completed_laps >= 0 and snapshot.completed_laps > self._last_completed_laps:
            if self._current_lap_buffer:
                self._write_lap_parquet_to_s3(
                    lap_number=self._last_completed_laps,
                    samples=self._current_lap_buffer,
                    car=snapshot.car,
                    track=snapshot.track,
                )
                self._current_lap_buffer = []

        self._last_completed_laps = snapshot.completed_laps

        if not snapshot.is_in_pit:
            parquet_row = self._flatten_for_parquet(snapshot)
            self._current_lap_buffer.append(parquet_row)

        self._row_count += 1

        if self._row_count % 600 == 0:
            logger.debug(f"  Streamed {self._row_count} telemetry rows to AWS")

    # ── Events ──

    def write_session_event(self, event: SessionEvent):
        """Buffer a session event for S3 upload."""
        if not self._current_session_id:
            return
        data = event.to_dict()
        data["session_id"] = self._current_session_id
        self._session_events.append(json.dumps(data))

    def write_lap_completion(self, event: LapCompletionEvent):
        """Buffer a lap completion event for S3 upload."""
        if not self._current_session_id:
            return
        data = event.to_dict()
        data["session_id"] = self._current_session_id
        self._lap_events.append(json.dumps(data))

    def write_setup_snapshot(self, event: SetupSnapshot):
        """Buffer a setup snapshot for S3 upload."""
        if not self._current_session_id:
            return
        data = event.to_dict()
        data["session_id"] = self._current_session_id
        self._setup_events.append(json.dumps(data))

    # ── Firehose Internals ──

    def _add_to_firehose_buffer(self, record_bytes: bytes):
        """Add a record to the Firehose buffer, flushing if batch limits are reached."""
        self._firehose_buffer.append(record_bytes)
        self._firehose_buffer_bytes += len(record_bytes)

        if (
            len(self._firehose_buffer) >= self._FIREHOSE_BATCH_SIZE
            or self._firehose_buffer_bytes >= self._FIREHOSE_BATCH_BYTES
        ):
            self._flush_firehose_buffer()

    def _flush_firehose_buffer(self):
        """Send buffered records to Kinesis Data Firehose using put_record_batch."""
        if not self._firehose_buffer:
            return

        records = [{"Data": b} for b in self._firehose_buffer]
        try:
            response = self._firehose.put_record_batch(
                DeliveryStreamName=FIREHOSE_STREAM_NAME,
                Records=records,
            )
            failed = response.get("FailedPutCount", 0)
            if failed > 0:
                self._firehose_failures += failed
                logger.warning(f"Firehose batch: {failed}/{len(records)} records failed")
        except Exception as e:
            self._firehose_failures += len(records)
            logger.error(f"Firehose batch send failed: {e}")

        self._firehose_buffer = []
        self._firehose_buffer_bytes = 0

    # ── S3 Parquet Writes ──

    def _write_lap_parquet_to_s3(self, lap_number: int, samples: List[dict], car: str, track: str):
        """Write a completed lap's samples as a Parquet file directly to S3."""
        if not samples:
            return

        s3_key = get_s3_parquet_key(
            platform="ACC",
            track=track,
            car=car,
            session_id=self._current_session_id,
            lap_number=lap_number,
        )

        # Build Arrow table
        columns = {col: [] for col in TELEMETRY_SCHEMA.names}
        for row in samples:
            for col in TELEMETRY_SCHEMA.names:
                columns[col].append(row.get(col))

        table = pa.table(columns, schema=TELEMETRY_SCHEMA)

        # Write to in-memory buffer, then upload to S3
        buf = io.BytesIO()
        pq.write_table(table, buf, compression="snappy")
        buf.seek(0)

        try:
            self._s3.put_object(
                Bucket=S3_BUCKET,
                Key=s3_key,
                Body=buf.getvalue(),
            )
            self._laps_written += 1
            size_kb = len(buf.getvalue()) / 1024
            logger.info(
                f"  ☁️  Wrote lap {lap_number} → s3://{S3_BUCKET}/{s3_key} "
                f"({len(samples)} samples, {size_kb:.1f} KB)"
            )
        except Exception as e:
            logger.error(f"Failed to upload lap {lap_number} Parquet to S3: {e}")

    def _flush_current_lap(self):
        """Write any remaining buffered lap data to S3."""
        if self._current_lap_buffer and self._current_session_id:
            last = self._current_lap_buffer[-1]
            self._write_lap_parquet_to_s3(
                lap_number=self._last_completed_laps if self._last_completed_laps >= 0 else 0,
                samples=self._current_lap_buffer,
                car=last.get("car", "unknown"),
                track=last.get("track", "unknown"),
            )
            self._current_lap_buffer = []

    # ── S3 Event Uploads ──

    def _upload_events_to_s3(self):
        """Upload session/lap/setup event JSONL files to S3."""
        event_groups = {
            "session_events.jsonl": self._session_events,
            "lap_completions.jsonl": self._lap_events,
            "setup_snapshots.jsonl": self._setup_events,
        }

        for filename, events in event_groups.items():
            if not events:
                continue

            s3_key = f"{S3_RAW_PREFIX}events/{self._current_session_id}/{filename}"
            body = "\n".join(events) + "\n"

            try:
                self._s3.put_object(
                    Bucket=S3_BUCKET,
                    Key=s3_key,
                    Body=body.encode("utf-8"),
                )
                logger.info(f"  ☁️  Uploaded {filename} ({len(events)} events) → s3://{S3_BUCKET}/{s3_key}")
            except Exception as e:
                logger.error(f"Failed to upload {filename} to S3: {e}")

    # ── Snapshot Flattening ──

    def _flatten_snapshot(self, s: ACCTelemetrySnapshot) -> Dict[str, Any]:
        """Flatten an ACCTelemetrySnapshot into a dictionary for JSON/Firehose."""
        return {
            "timestamp_wall": s.timestamp_wall.isoformat(),
            "timestamp_game": s.timestamp_game,
            "car": s.car,
            "track": s.track,
            "session_type": s.session_type,
            "status": s.status,

            "completed_laps": s.completed_laps,
            "current_lap_time_ms": s.current_lap_time_ms,
            "last_lap_time_ms": s.last_lap_time_ms,
            "best_lap_time_ms": s.best_lap_time_ms,
            "distance_into_lap": s.distance_into_lap,
            "normalized_position": s.normalized_position,
            "current_sector": s.current_sector,
            "last_sector_time_ms": s.last_sector_time_ms,
            "is_valid_lap": s.is_valid_lap,
            "is_in_pit": s.is_in_pit,
            "is_in_pit_lane": s.is_in_pit_lane,

            "throttle": s.throttle,
            "brake": s.brake,
            "steering": s.steering,
            "clutch": s.clutch,

            "speed_kmh": s.speed_kmh,
            "gear": s.gear,
            "rpm": s.rpm,
            "fuel_remaining": s.fuel_remaining,
            "fuel_per_lap": s.fuel_per_lap,

            "g_lat": s.g_lat,
            "g_lon": s.g_lon,
            "g_vert": s.g_vert,

            "tyre_pressure_fl": s.wheel_fl.pressure,
            "tyre_pressure_fr": s.wheel_fr.pressure,
            "tyre_pressure_rl": s.wheel_rl.pressure,
            "tyre_pressure_rr": s.wheel_rr.pressure,

            "tyre_temp_fl": s.wheel_fl.core_temp,
            "tyre_temp_fr": s.wheel_fr.core_temp,
            "tyre_temp_rl": s.wheel_rl.core_temp,
            "tyre_temp_rr": s.wheel_rr.core_temp,

            "tyre_inner_temp_fl": s.wheel_fl.inner_temp,
            "tyre_inner_temp_fr": s.wheel_fr.inner_temp,
            "tyre_inner_temp_rl": s.wheel_rl.inner_temp,
            "tyre_inner_temp_rr": s.wheel_rr.inner_temp,

            "tyre_middle_temp_fl": s.wheel_fl.middle_temp,
            "tyre_middle_temp_fr": s.wheel_fr.middle_temp,
            "tyre_middle_temp_rl": s.wheel_rl.middle_temp,
            "tyre_middle_temp_rr": s.wheel_rr.middle_temp,

            "tyre_outer_temp_fl": s.wheel_fl.outer_temp,
            "tyre_outer_temp_fr": s.wheel_fr.outer_temp,
            "tyre_outer_temp_rl": s.wheel_rl.outer_temp,
            "tyre_outer_temp_rr": s.wheel_rr.outer_temp,

            "tyre_slip_fl": s.wheel_fl.slip,
            "tyre_slip_fr": s.wheel_fr.slip,
            "tyre_slip_rl": s.wheel_rl.slip,
            "tyre_slip_rr": s.wheel_rr.slip,

            "suspension_travel_fl": s.wheel_fl.suspension_travel,
            "suspension_travel_fr": s.wheel_fr.suspension_travel,
            "suspension_travel_rl": s.wheel_rl.suspension_travel,
            "suspension_travel_rr": s.wheel_rr.suspension_travel,

            "brake_temp_fl": s.wheel_fl.brake_temp,
            "brake_temp_fr": s.wheel_fr.brake_temp,
            "brake_temp_rl": s.wheel_rl.brake_temp,
            "brake_temp_rr": s.wheel_rr.brake_temp,

            "tyre_wear_fl": s.wheel_fl.wear,
            "tyre_wear_fr": s.wheel_fr.wear,
            "tyre_wear_rl": s.wheel_rl.wear,
            "tyre_wear_rr": s.wheel_rr.wear,

            "tc_level": s.tc_level,
            "tc_cut": s.tc_cut,
            "abs_level": s.abs_level,
            "engine_map": s.engine_map,
            "brake_bias": s.brake_bias,

            "air_temp": s.air_temp,
            "road_temp": s.road_temp,
            "track_grip": s.track_grip,
            "rain_intensity": s.rain_intensity,
            "wind_speed": s.wind_speed,
            "wind_direction": s.wind_direction,
        }

    def _flatten_for_parquet(self, s: ACCTelemetrySnapshot) -> dict:
        """Flatten an ACCTelemetrySnapshot into a Parquet row dict."""
        return {
            "timestamp_wall": s.timestamp_wall,
            "timestamp_game": s.timestamp_game,
            "session_id": self._current_session_id,
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

    def close(self):
        """Clean up."""
        if self._current_session_id:
            self.end_session()
