"""
Test Firehose Ingestion

Sends 500 records of mock data through the AWSPublisher to verify
Kinesis Data Firehose is actively receiving and buffering data.
"""

import sys
import time
import json
from pathlib import Path
from loguru import logger

# Fix Windows console encoding for emoji output
sys.stdout.reconfigure(encoding="utf-8", errors="replace")

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.listener.aws_publisher import AWSPublisher
from src.listener.acc_reader import ACCTelemetrySnapshot
from config.settings import AWS_REGION, S3_BUCKET, FIREHOSE_STREAM_NAME, RAW_DATA_DIR

def run_firehose_test():
    session_id = "acc_20260330_monza_gt3_sample"
    csv_path = RAW_DATA_DIR / session_id / "telemetry.csv"

    if not csv_path.exists():
        logger.error("Sample data not found. Run generate_sample_data.py first.")
        return

    print("=" * 60)
    print(f"🔥 Testing AWS Firehose Ingestion")
    print(f"   Stream: {FIREHOSE_STREAM_NAME}")
    print(f"   Region: {AWS_REGION}")
    print("=" * 60)

    publisher = AWSPublisher()
    publisher.start_session("test_firehose_session_001")

    # Read 500 rows and send them through the publisher
    logger.info("Reading 500 lines of mock telemetry...")
    
    count = 0
    with open(csv_path, 'r', encoding='utf-8') as f:
        # Skip header
        header = f.readline().strip().split(',')
        
        for line in f:
            if count >= 500:
                break
                
            values = line.strip().split(',')
            row_dict = dict(zip(header, values))
            
            # The publisher expects a snapshot object. 
            # For this test, we will bypass the snapshot object and dump directly into the firehose buffer 
            # to avoid writing a massive conversion function just for the test.
            
            row_dict["session_id"] = "test_firehose_session_001"
            json_line = json.dumps(row_dict) + "\n"
            
            publisher._add_to_firehose_buffer(json_line.encode("utf-8"))
            count += 1

    logger.info(f"Buffered {count} records. Forcing flush to Firehose...")
    
    # Force flush the buffer to Firehose
    publisher._flush_firehose_buffer()
    
    logger.info("✅ Data successfully sent to Firehose!")
    print("=" * 60)
    print("What to do next:")
    print("1. Go to the AWS Console -> Kinesis -> Data Firehose")
    print(f"2. Click on '{FIREHOSE_STREAM_NAME}'")
    print("3. Check the 'Monitoring' tab. You should see incoming bytes/records spike shortly.")
    print("   Note: CloudWatch metrics can be delayed by 1-2 minutes.")
    print("4. Firehose is configured to buffer for 60 seconds or 1MB.")
    print("   In about 1 minute, check your S3 bucket:")
    print(f"   s3://{S3_BUCKET}/raw/ (You should see a new file appear!)")
    print("=" * 60)

if __name__ == "__main__":
    run_firehose_test()
