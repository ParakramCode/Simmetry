"""
Central configuration for the Sim Racing Telemetry Platform.
All paths, connection strings, and domain constants in one place.
"""

import os
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()


# ─── Project Paths ──────────────────────────────────────────────
PROJECT_ROOT = Path(__file__).parent.parent
DATA_DIR = PROJECT_ROOT / "data"
RAW_DATA_DIR = DATA_DIR / "raw"
TELEMETRY_DIR = DATA_DIR / "telemetry"
SAMPLE_DATA_DIR = DATA_DIR / "sample"

# Ensure data directories exist
for d in [RAW_DATA_DIR, TELEMETRY_DIR, SAMPLE_DATA_DIR]:
    d.mkdir(parents=True, exist_ok=True)


# ─── ACC Shared Memory ─────────────────────────────────────────
ACC_PHYSICS_TAG = "Local\\acpmf_physics"
ACC_GRAPHICS_TAG = "Local\\acpmf_graphics"
ACC_STATIC_TAG = "Local\\acpmf_static"

# Polling rate — 30Hz (matches Graphics update rate per SDK v1.8.12)
# Physics updates at 60Hz, Graphics at 30Hz — we poll at the slower rate
POLLING_RATE_HZ = 30
POLLING_INTERVAL_S = 1.0 / POLLING_RATE_HZ  # ~33.33ms


# ─── AWS ───────────────────────────────────────────────────────
AWS_REGION = os.getenv("AWS_REGION", "us-east-1")
S3_BUCKET = os.getenv("S3_BUCKET", "sim-telemetry-lake")
S3_RAW_PREFIX = os.getenv("S3_RAW_PREFIX", "raw/")
S3_PROCESSED_PREFIX = os.getenv("S3_PROCESSED_PREFIX", "processed/")
FIREHOSE_STREAM_NAME = os.getenv("FIREHOSE_STREAM_NAME", "telemetry-firehose")
ATHENA_DATABASE = os.getenv("ATHENA_DATABASE", "sim_telemetry")
ATHENA_RESULTS_BUCKET = os.getenv("ATHENA_RESULTS_BUCKET", f"s3://{S3_BUCKET}/athena-results/")


# ─── Tyre Operating Window (by compound) ──────────────────────
TYRE_OPTIMAL_WINDOWS = {
    "gt3_dry": {"min_temp": 80.0, "max_temp": 100.0},
    "gt3_wet": {"min_temp": 50.0, "max_temp": 70.0},
    "gt4_dry": {"min_temp": 75.0, "max_temp": 95.0},
}


def get_s3_parquet_key(platform: str, track: str, car: str, session_id: str, lap_number: int) -> str:
    """
    Build the S3 object key for a given lap's Parquet file.
    Convention: processed/{platform}/{track}/{car}/{session_id}/lap_{NNN}.parquet
    """
    car_safe = car.replace(" ", "_")
    track_safe = track.replace(" ", "_")
    return f"{S3_PROCESSED_PREFIX}{platform}/{track_safe}/{car_safe}/{session_id}/lap_{lap_number:03d}.parquet"


# ─── Session ID Generation ────────────────────────────────────
def generate_session_id(platform: str, track: str, car_class: str) -> str:
    """
    Generate a unique session ID.
    Format: {platform}_{YYYYMMDD_HHMMSS}_{track}_{class}
    """
    from datetime import datetime
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    track_safe = track.lower().replace(" ", "_")
    return f"{platform.lower()}_{ts}_{track_safe}_{car_class.lower()}"
