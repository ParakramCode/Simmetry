"""
Kafka Telemetry Publisher — Week 2

Publishes the ACC telemetry snapshot to a Kafka stream.
Uses `confluent-kafka` for high-throughput serialization.
"""

import json
from datetime import datetime
from typing import Optional, Dict, Any
from loguru import logger
from confluent_kafka import Producer

from .acc_reader import ACCTelemetrySnapshot
from .session_manager import SessionEvent, LapCompletionEvent, SetupChangeEvent, SetupSnapshot
from config.settings import KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPICS

class KafkaTelemetryPublisher:
    """
    Publishes ACCTelemetrySnapshot and Session events to Kafka.
    """
    
    def __init__(self):
        self._producer = Producer({
            'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
            'client.id': 'acc_telemetry_listener',
            'linger.ms': 10, # batching
            'compression.type': 'snappy',
            'acks': '1', # leader acknowledgment
        })
        self._telemetry_topic = KAFKA_TOPICS['telemetry_raw']['name']
        self._session_topic = KAFKA_TOPICS['session_events']['name']
        self._lap_topic = KAFKA_TOPICS['lap_completions']['name']
        self._setup_topic = KAFKA_TOPICS['setup_snapshots']['name']
        
        # In multi-car or multi-user setup, we'd use session_id as the partition key.
        self._current_session_id: Optional[str] = None
        self._row_count = 0
        self._delivery_failures = 0
        
        logger.info(f"KafkaTelemetryPublisher connecting to {KAFKA_BOOTSTRAP_SERVERS}")

    def _delivery_callback(self, err, msg):
        """Called once for each message produced to indicate delivery result."""
        if err is not None:
            self._delivery_failures += 1
            if self._delivery_failures % 100 == 1:
                logger.error(f"Message delivery failed: {err}")

    def start_session(self, session_id: str):
        """Initialize publisher for a new session."""
        self._current_session_id = session_id
        self._row_count = 0
        self._delivery_failures = 0
        logger.info(f"🛰️ Streaming data to Kafka for session: {session_id}")

    def write_telemetry(self, snapshot: ACCTelemetrySnapshot):
        """Publish telemetry reading to Kafka."""
        if not self._current_session_id:
            return
            
        data = self._flatten_snapshot(snapshot)
        data['session_id'] = self._current_session_id
        
        try:
            # We use session_id as the key to ensure order within the same session
            self._producer.produce(
                topic=self._telemetry_topic,
                key=self._current_session_id.encode('utf-8'),
                value=json.dumps(data).encode('utf-8'),
                on_delivery=self._delivery_callback
            )
            self._row_count += 1
            
            # Poll handles delivery reports (callbacks)
            self._producer.poll(0)
            
            if self._row_count % 600 == 0:
                logger.debug(f"  Streamed {self._row_count} telemetry rows to Kafka")
                
        except BufferError:
            logger.warning("Kafka producer local queue full; waiting...")
            self._producer.poll(0.1)
            self._producer.produce(
                topic=self._telemetry_topic,
                key=self._current_session_id.encode('utf-8'),
                value=json.dumps(data).encode('utf-8'),
                on_delivery=self._delivery_callback
            )

    def write_session_event(self, event: SessionEvent):
        """Write session start/end to Kafka."""
        if not self._current_session_id:
            return
            
        data = event.to_dict()
        data['session_id'] = self._current_session_id
        self._producer.produce(
            topic=self._session_topic,
            key=self._current_session_id.encode('utf-8'),
            value=json.dumps(data).encode('utf-8'),
            on_delivery=self._delivery_callback
        )
        self._producer.poll(0)

    def write_lap_completion(self, event: LapCompletionEvent):
        """Write a lap completion event to Kafka."""
        if not self._current_session_id:
            return
            
        data = event.to_dict()
        data['session_id'] = self._current_session_id
        self._producer.produce(
            topic=self._lap_topic,
            key=self._current_session_id.encode('utf-8'),
            value=json.dumps(data).encode('utf-8'),
            on_delivery=self._delivery_callback
        )
        self._producer.poll(0)

    def write_setup_snapshot(self, event: SetupSnapshot):
        """Write a setup change snapshot to Kafka."""
        if not self._current_session_id:
            return
            
        data = event.to_dict()
        data['session_id'] = self._current_session_id
        self._producer.produce(
            topic=self._setup_topic,
            key=self._current_session_id.encode('utf-8'),
            value=json.dumps(data).encode('utf-8'),
            on_delivery=self._delivery_callback
        )
        self._producer.poll(0)

    def end_session(self):
        """Finish session and flush all remaining messages to Kafka."""
        logger.info(f"🛰️ Session {self._current_session_id} ending. Flushing Kafka producer...")
        self._producer.flush(timeout=5.0)
        self._current_session_id = None
        self._row_count = 0
        
    def _flatten_snapshot(self, s: ACCTelemetrySnapshot) -> Dict[str, Any]:
        """Flatten an ACCTelemetrySnapshot into a dictionary suitable for JSON serialization.
        Note: Unlike CSV, we keep floats as floats here for efficient typed processing in Spark."""
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

    def close(self):
        """Clean up."""
        if self._current_session_id:
            self.end_session()
