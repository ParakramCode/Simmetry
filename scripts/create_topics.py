"""
Kafka Topic Creation Script

Creates the necessary Kafka topics defined in config/settings.py.
Requires the Kafka broker defined in docker-compose.yml to be running.
"""

import sys
from pathlib import Path
from loguru import logger
from confluent_kafka.admin import AdminClient, NewTopic

# Add the project root to sys.path so we can import config
sys.path.append(str(Path(__file__).parent.parent))

from config.settings import KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPICS

def create_kafka_topics():
    """Create all configured Kafka topics."""
    logger.info(f"Connecting to Kafka admin client at {KAFKA_BOOTSTRAP_SERVERS}...")
    admin_client = AdminClient({"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS})
    
    # List existing topics
    metadata = admin_client.list_topics(timeout=10)
    existing_topics = set(metadata.topics.keys())
    
    new_topics = []
    
    for topic_key, config in KAFKA_TOPICS.items():
        topic_name = config["name"]
        
        if topic_name in existing_topics:
            logger.info(f"Topic '{topic_name}' already exists. Skipping.")
            continue
            
        logger.info(f"Preparing to create topic '{topic_name}' (partitions: {config['partitions']})")
        
        topic_config = {}
        if config["retention_ms"] > 0:
            topic_config["retention.ms"] = str(config["retention_ms"])
            
        new_topic = NewTopic(
            topic=topic_name,
            num_partitions=config["partitions"],
            replication_factor=1,
            config=topic_config
        )
        new_topics.append(new_topic)
        
    if not new_topics:
        logger.success("All topics are already created.")
        return
        
    # Create topics
    fs = admin_client.create_topics(new_topics)
    
    # Wait for completion
    for topic, f in fs.items():
        try:
            f.result(timeout=10)
            logger.success(f"✅ Topic '{topic}' created successfully.")
        except Exception as e:
            logger.error(f"❌ Failed to create topic '{topic}': {e}")

if __name__ == "__main__":
    try:
        create_kafka_topics()
    except Exception as e:
        logger.error(f"Error connecting to Kafka: {e}")
        logger.error("Make sure you have run 'docker-compose up -d' first!")
