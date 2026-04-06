"""
Kafka to PostgreSQL Consumer — Week 4

Runs continuously alongside the Spark Streaming job.
Subscribes to session_events, lap_completions, and setup_snapshots Kafka topics.
Deserializes the JSON events and inserts them directly into the PostgreSQL database using SQLAlchemy.
"""

import json
import sys
import time
from pathlib import Path
from loguru import logger
from confluent_kafka import Consumer, KafkaError

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

# Add project root to sys.path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from config.settings import KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPICS, DATABASE_URL
from src.storage.db import Session, Lap, SetupSnapshotDB

def start_consumer():
    """Start listening to Kafka events and inserting to DB."""
    logger.info(f"Connecting to database at {DATABASE_URL}...")
    engine = create_engine(DATABASE_URL)
    SessionMaker = sessionmaker(bind=engine)

    logger.info(f"Connecting Consumer to Kafka at {KAFKA_BOOTSTRAP_SERVERS}...")
    consumer = Consumer({
        'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
        'group.id': 'telemetry_storage_group',
        'auto.offset.reset': 'earliest'
    })

    topics = [
        KAFKA_TOPICS['session_events']['name'],
        KAFKA_TOPICS['lap_completions']['name'],
        KAFKA_TOPICS['setup_snapshots']['name']
    ]
    
    consumer.subscribe(topics)
    logger.info(f"🎧 Subscribed to topics: {topics}")

    try:
        while True:
            msg = consumer.poll(1.0)

            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    logger.error(f"Consumer error: {msg.error()}")
                    break

            # Parse event
            topic = msg.topic()
            try:
                payload = json.loads(msg.value().decode('utf-8'))
            except Exception as e:
                logger.error(f"Failed to json payload: {e}")
                continue

            with SessionMaker() as db:
                try:
                    if topic == KAFKA_TOPICS['session_events']['name']:
                        event_type = payload.get("event_type")
                        if event_type == "session_start":
                            logger.info(f"📦 Storing new session: {payload['session_id']}")
                            new_session = Session(
                                id=payload["session_id"],
                                platform=payload["platform"],
                                track=payload["track"],
                                car=payload["car"],
                                session_type=payload["session_type"]
                            )
                            db.add(new_session)
                        elif event_type == "session_end":
                            logger.info(f"🏁 Updating session end: {payload['session_id']}")
                            sess = db.query(Session).filter_by(id=payload["session_id"]).first()
                            if sess:
                                sess.total_laps = payload.get("total_laps", sess.total_laps)
                                sess.best_lap_ms = payload.get("best_lap_ms", sess.best_lap_ms)
                    
                    elif topic == KAFKA_TOPICS['lap_completions']['name']:
                        logger.info(f"⏱️ Storing lap {payload['lap_number']} for session {payload['session_id']}")
                        new_lap = Lap(
                            session_id=payload["session_id"],
                            lap_number=payload["lap_number"],
                            lap_time_ms=payload["lap_time_ms"],
                            sector_1_ms=payload["sector_1_ms"],
                            sector_2_ms=payload["sector_2_ms"],
                            sector_3_ms=payload["sector_3_ms"],
                            is_valid=payload["is_valid"],
                            fuel_used=payload["fuel_used"],
                            fuel_remaining=payload["fuel_remaining"],
                            avg_tyre_temp_fl=payload["avg_tyre_temp_fl"],
                            avg_tyre_temp_fr=payload["avg_tyre_temp_fr"],
                            avg_tyre_temp_rl=payload["avg_tyre_temp_rl"],
                            avg_tyre_temp_rr=payload["avg_tyre_temp_rr"]
                        )
                        db.add(new_lap)
                        
                    elif topic == KAFKA_TOPICS['setup_snapshots']['name']:
                        logger.info(f"🔧 Storing setup snapshot on lap {payload.get('lap_number', 0)} for session {payload['session_id']}")
                        new_setup = SetupSnapshotDB(
                            session_id=payload["session_id"],
                            lap_number=payload.get("lap_number", 0),
                            tc_level=payload["tc_level"],
                            tc_cut=payload["tc_cut"],
                            abs_level=payload["abs_level"],
                            engine_map=payload["engine_map"],
                            brake_bias=payload["brake_bias"]
                        )
                        db.add(new_setup)
                        
                    db.commit()
                except Exception as e:
                    logger.error(f"Error processing {topic} message: {e}")
                    db.rollback()

    except KeyboardInterrupt:
        logger.info("Stopping storage consumer...")
    finally:
        consumer.close()

if __name__ == "__main__":
    start_consumer()
