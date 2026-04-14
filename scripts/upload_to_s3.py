"""
Upload Sample Data to S3 — Bootstraps the AWS data lake.

Takes the locally generated sample data (from generate_sample_data.py)
and uploads it into the S3 bucket as JSON lines, mimicking what
Kinesis Data Firehose would produce in a live session.

This allows you to test the full AWS pipeline (Athena queries, Glue ETL,
dashboards) without needing ACC to be running.

Prerequisites:
  1. Run `python scripts/generate_sample_data.py` first to create local data
  2. Configure AWS credentials: `aws configure`
  3. Set S3_BUCKET in your .env file

Usage:
    python scripts/upload_to_s3.py
"""

import csv
import json
import sys
from pathlib import Path

# Fix Windows console encoding for emoji output
sys.stdout.reconfigure(encoding="utf-8", errors="replace")

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

import boto3
from config.settings import AWS_REGION, S3_BUCKET, S3_RAW_PREFIX, RAW_DATA_DIR

SESSION_ID = "acc_20260330_monza_gt3_sample"


def upload_telemetry_csv_as_json(s3_client, csv_path: Path):
    """
    Read the local telemetry CSV, convert each row to JSON,
    and upload as batched JSONL files to S3 (matching Firehose output format).
    """
    print(f"📖 Reading: {csv_path}")

    rows = []
    with open(csv_path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            rows.append(row)

    print(f"   Total rows: {len(rows):,}")

    # Batch into ~1MB files (like Firehose would)
    BATCH_SIZE = 5000  # rows per file
    batch_num = 0

    for i in range(0, len(rows), BATCH_SIZE):
        batch = rows[i : i + BATCH_SIZE]
        json_lines = "\n".join(json.dumps(row) for row in batch) + "\n"

        s3_key = f"{S3_RAW_PREFIX}{SESSION_ID}/batch_{batch_num:04d}.json"

        s3_client.put_object(
            Bucket=S3_BUCKET,
            Key=s3_key,
            Body=json_lines.encode("utf-8"),
        )

        batch_num += 1
        print(f"   ☁️  Uploaded batch {batch_num}: {len(batch)} rows → s3://{S3_BUCKET}/{s3_key}")

    print(f"   ✅ Uploaded {len(rows):,} rows in {batch_num} batches")
    return len(rows)


def upload_event_files(s3_client, session_dir: Path):
    """Upload the session event JSONL files to S3."""
    event_files = ["session_events.jsonl", "lap_completions.jsonl", "setup_snapshots.jsonl"]

    for filename in event_files:
        local_path = session_dir / filename
        if not local_path.exists():
            print(f"   ⚠️  Skipping {filename} (not found)")
            continue

        s3_key = f"{S3_RAW_PREFIX}events/{SESSION_ID}/{filename}"
        with open(local_path, "rb") as f:
            s3_client.put_object(
                Bucket=S3_BUCKET,
                Key=s3_key,
                Body=f.read(),
            )
        print(f"   ☁️  Uploaded {filename} → s3://{S3_BUCKET}/{s3_key}")


def main():
    session_dir = RAW_DATA_DIR / SESSION_ID

    if not session_dir.exists():
        print("❌ Sample data not found. Run this first:")
        print("   python scripts/generate_sample_data.py")
        sys.exit(1)

    print(f"☁️  Uploading sample data to S3")
    print(f"   Bucket: {S3_BUCKET}")
    print(f"   Region: {AWS_REGION}")
    print(f"   Source: {session_dir}")
    print()

    s3_client = boto3.client("s3", region_name=AWS_REGION)

    # Check bucket exists
    try:
        s3_client.head_bucket(Bucket=S3_BUCKET)
        print(f"   ✅ Bucket '{S3_BUCKET}' found")
    except Exception as e:
        print(f"   ❌ Cannot access bucket '{S3_BUCKET}': {e}")
        print(f"   Create it first in the AWS Console or run:")
        print(f"   aws s3 mb s3://{S3_BUCKET} --region {AWS_REGION}")
        sys.exit(1)

    print()

    # Upload telemetry
    csv_path = session_dir / "telemetry.csv"
    if csv_path.exists():
        upload_telemetry_csv_as_json(s3_client, csv_path)
    else:
        print("❌ telemetry.csv not found in sample data directory")
        sys.exit(1)

    print()

    # Upload events
    upload_event_files(s3_client, session_dir)

    print()
    print("=" * 60)
    print("✅ Upload complete!")
    print()
    print("Next steps:")
    print("  1. Go to Amazon Athena in the AWS Console")
    print("  2. Run: python -c \"from src.storage.athena_client import AthenaClient; AthenaClient().create_tables()\"")
    print("  3. Query your data: SELECT * FROM sim_telemetry.telemetry_raw LIMIT 10")
    print("=" * 60)


if __name__ == "__main__":
    main()
