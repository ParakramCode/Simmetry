"""
AWS Infrastructure Setup — Creates all required AWS resources.

Creates:
  1. S3 Bucket (data lake)
  2. Kinesis Data Firehose delivery stream → S3
  3. IAM Role for Firehose

Prerequisites:
  - AWS CLI configured: `aws configure`
  - Your IAM user must have permissions for S3, Firehose, IAM

Usage:
    python scripts/setup_aws.py
"""

import json
import sys
import time
from pathlib import Path

# Fix Windows console encoding for emoji output
sys.stdout.reconfigure(encoding="utf-8", errors="replace")

sys.path.insert(0, str(Path(__file__).parent.parent))

import boto3
from botocore.exceptions import ClientError
from config.settings import AWS_REGION, S3_BUCKET, FIREHOSE_STREAM_NAME, S3_RAW_PREFIX

# ── Colors for terminal ──
GREEN = "\033[92m"
RED = "\033[91m"
YELLOW = "\033[93m"
RESET = "\033[0m"


def create_s3_bucket(s3_client):
    """Create the S3 bucket for the data lake."""
    print(f"\n{'='*50}")
    print(f"Step 1: Creating S3 Bucket: {S3_BUCKET}")
    print(f"{'='*50}")

    try:
        if AWS_REGION == "us-east-1":
            s3_client.create_bucket(Bucket=S3_BUCKET)
        else:
            s3_client.create_bucket(
                Bucket=S3_BUCKET,
                CreateBucketConfiguration={"LocationConstraint": AWS_REGION},
            )
        print(f"{GREEN}✅ Bucket '{S3_BUCKET}' created{RESET}")
    except ClientError as e:
        error_code = e.response["Error"]["Code"]
        if error_code in ("BucketAlreadyOwnedByYou", "BucketAlreadyExists"):
            print(f"{YELLOW}⚠️  Bucket '{S3_BUCKET}' already exists — using it{RESET}")
        else:
            print(f"{RED}❌ Failed to create bucket: {e}{RESET}")
            return False

    # Create folder prefixes
    for prefix in ["raw/", "processed/", "athena-results/", "scripts/", "events/"]:
        s3_client.put_object(Bucket=S3_BUCKET, Key=prefix, Body=b"")
    print(f"   Created folder prefixes: raw/, processed/, athena-results/, scripts/")

    return True


def create_firehose_role(iam_client):
    """Create the IAM role that Firehose uses to write to S3."""
    print(f"\n{'='*50}")
    print(f"Step 2: Creating IAM Role for Firehose")
    print(f"{'='*50}")

    role_name = "telemetry-firehose-role"

    # Trust policy — allows Firehose service to assume this role
    trust_policy = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Principal": {"Service": "firehose.amazonaws.com"},
                "Action": "sts:AssumeRole",
            }
        ],
    }

    # Permission policy — allows writing to our S3 bucket
    s3_policy = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Action": [
                    "s3:AbortMultipartUpload",
                    "s3:GetBucketLocation",
                    "s3:GetObject",
                    "s3:ListBucket",
                    "s3:ListBucketMultipartUploads",
                    "s3:PutObject",
                ],
                "Resource": [
                    f"arn:aws:s3:::{S3_BUCKET}",
                    f"arn:aws:s3:::{S3_BUCKET}/*",
                ],
            }
        ],
    }

    try:
        response = iam_client.create_role(
            RoleName=role_name,
            AssumeRolePolicyDocument=json.dumps(trust_policy),
            Description="Allows Kinesis Firehose to write to S3 telemetry bucket",
        )
        role_arn = response["Role"]["Arn"]
        print(f"{GREEN}✅ IAM Role created: {role_arn}{RESET}")

        # Attach inline policy
        iam_client.put_role_policy(
            RoleName=role_name,
            PolicyName="telemetry-firehose-s3-access",
            PolicyDocument=json.dumps(s3_policy),
        )
        print(f"   Attached S3 access policy")

        # IAM role propagation takes a few seconds
        print(f"   Waiting 10s for IAM role to propagate...")
        time.sleep(10)

    except ClientError as e:
        if e.response["Error"]["Code"] == "EntityAlreadyExists":
            print(f"{YELLOW}⚠️  IAM Role '{role_name}' already exists — reusing{RESET}")
            response = iam_client.get_role(RoleName=role_name)
            role_arn = response["Role"]["Arn"]
        else:
            print(f"{RED}❌ Failed to create IAM role: {e}{RESET}")
            return None

    return role_arn


def create_firehose_stream(firehose_client, role_arn: str):
    """Create the Kinesis Data Firehose delivery stream."""
    print(f"\n{'='*50}")
    print(f"Step 3: Creating Firehose Stream: {FIREHOSE_STREAM_NAME}")
    print(f"{'='*50}")

    try:
        firehose_client.create_delivery_stream(
            DeliveryStreamName=FIREHOSE_STREAM_NAME,
            DeliveryStreamType="DirectPut",
            S3DestinationConfiguration={
                "RoleARN": role_arn,
                "BucketARN": f"arn:aws:s3:::{S3_BUCKET}",
                "Prefix": S3_RAW_PREFIX,
                "ErrorOutputPrefix": "errors/",
                "BufferingHints": {
                    "SizeInMBs": 1,         # Buffer up to 1MB before writing
                    "IntervalInSeconds": 60,  # Or write every 60 seconds
                },
                "CompressionFormat": "UNCOMPRESSED",  # Keep as JSON for Athena
            },
        )
        print(f"{GREEN}✅ Firehose stream '{FIREHOSE_STREAM_NAME}' created{RESET}")
        print(f"   Buffer: 1 MB or 60 seconds")
        print(f"   Destination: s3://{S3_BUCKET}/{S3_RAW_PREFIX}")
    except ClientError as e:
        if "already exists" in str(e).lower() or e.response["Error"]["Code"] == "ResourceInUseException":
            print(f"{YELLOW}⚠️  Firehose stream already exists — using it{RESET}")
        else:
            print(f"{RED}❌ Failed to create Firehose: {e}{RESET}")
            return False

    return True


def main():
    print("☁️  AWS Infrastructure Setup for Sim Racing Telemetry")
    print(f"   Region: {AWS_REGION}")
    print(f"   Bucket: {S3_BUCKET}")
    print(f"   Firehose: {FIREHOSE_STREAM_NAME}")

    s3 = boto3.client("s3", region_name=AWS_REGION)
    iam = boto3.client("iam", region_name=AWS_REGION)
    firehose = boto3.client("firehose", region_name=AWS_REGION)

    # Step 1: S3
    if not create_s3_bucket(s3):
        print(f"\n{RED}Setup aborted.{RESET}")
        sys.exit(1)

    # Step 2: IAM Role
    role_arn = create_firehose_role(iam)
    if not role_arn:
        print(f"\n{RED}Setup aborted.{RESET}")
        sys.exit(1)

    # Step 3: Firehose
    create_firehose_stream(firehose, role_arn)

    print(f"\n{'='*60}")
    print(f"{GREEN}✅ AWS infrastructure setup complete!{RESET}")
    print()
    print("Next steps:")
    print(f"  1. Update your .env file:")
    print(f"     S3_BUCKET={S3_BUCKET}")
    print(f"     FIREHOSE_STREAM_NAME={FIREHOSE_STREAM_NAME}")
    print()
    print(f"  2. Upload sample data:")
    print(f"     python scripts/generate_sample_data.py")
    print(f"     python scripts/upload_to_s3.py")
    print()
    print(f"  3. Create Athena tables:")
    print(f'     python -c "from src.storage.athena_client import AthenaClient; AthenaClient().create_tables()"')
    print()
    print(f"  4. Start the listener:")
    print(f"     python -m src.listener.main")
    print(f"{'='*60}")


if __name__ == "__main__":
    main()
