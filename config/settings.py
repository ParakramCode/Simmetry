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


# ─── Kafka ─────────────────────────────────────────────────────
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

KAFKA_TOPICS = {
    "telemetry_raw": {
        "name": "telemetry_raw",
        "partitions": 3,
        "retention_ms": 86_400_000,   # 24 hours
    },
    "session_events": {
        "name": "session_events",
        "partitions": 1,
        "retention_ms": 2_592_000_000,  # 30 days
    },
    "lap_completions": {
        "name": "lap_completions",
        "partitions": 1,
        "retention_ms": 2_592_000_000,  # 30 days
    },
    "setup_snapshots": {
        "name": "setup_snapshots",
        "partitions": 1,
        "retention_ms": -1,  # indefinite
    },
}


# ─── PostgreSQL ────────────────────────────────────────────────
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
POSTGRES_PORT = int(os.getenv("POSTGRES_PORT", "5432"))
POSTGRES_DB = os.getenv("POSTGRES_DB", "telemetry")
POSTGRES_USER = os.getenv("POSTGRES_USER", "telemetry")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "telemetry_pass")

DATABASE_URL = (
    f"postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}"
    f"@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
)


# ─── Tyre Operating Window (by compound) ──────────────────────
TYRE_OPTIMAL_WINDOWS = {
    "gt3_dry": {"min_temp": 80.0, "max_temp": 100.0},
    "gt3_wet": {"min_temp": 50.0, "max_temp": 70.0},
    "gt4_dry": {"min_temp": 75.0, "max_temp": 95.0},
}


# ─── Parquet File Convention ───────────────────────────────────
def get_parquet_path(platform: str, track: str, car: str, session_id: str, lap_number: int) -> Path:
    """
    Build the parquet file path for a given lap.
    Convention: telemetry/{platform}/{track}/{car}/{session_id}/lap_{NNN}.parquet
    """
    car_safe = car.replace(" ", "_")
    track_safe = track.replace(" ", "_")
    path = TELEMETRY_DIR / platform / track_safe / car_safe / session_id / f"lap_{lap_number:03d}.parquet"
    path.parent.mkdir(parents=True, exist_ok=True)
    return path


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
