import json
from typing import Dict, Any, List
from src.setup_engine.models import TelemetrySymptoms
from src.setup_engine.engine import SetupEngine

# In a real environment, these would query the actual telemetry backend (Athena/PostgreSQL/Parquet).
# Here we mock the responses for the Agent to process.

def get_last_session() -> Dict[str, Any]:
    """Returns metadata about the most recent telemetry session."""
    return {
        "session_id": "acc_20260330_monza_gt3_sample",
        "track": "Monza",
        "car": "Ferrari 296 GT3",
        "total_laps": 10,
        "best_lap_ms": 107400, # 1:47.400
        "conditions": "optimum"
    }

def get_lap_summary(lap_id: int) -> Dict[str, Any]:
    """Retrieves sector times, valid status, and overall time for a specific lap."""
    return {
        "lap_id": lap_id,
        "lap_time_ms": 108100,
        "sectors": [28400, 39600, 40100],
        "is_valid": True,
        "fuel_used": 2.6
    }

def compare_laps(lap_a: int, lap_b: int) -> Dict[str, Any]:
    """Returns delta metrics between two laps."""
    return {
        "lap_a": lap_a,
        "lap_b": lap_b,
        "time_delta_ms": -450, # lap_a was 0.45s faster
        "sector_deltas": [-100, -250, -100],
        "key_difference": "Braking later into Turn 1 (Sector 1) and better exit out of Ascari (Sector 2)."
    }

def get_driver_consistency(session_id: str) -> Dict[str, Any]:
    """Analyzes variance across lap times to score consistency."""
    return {
        "session_id": session_id,
        "consistency_score": 85, # out of 100
        "avg_variance_ms": 300,
        "notes": "Very consistent in Sector 1. Varying braking points into Parabolica in Sector 3."
    }

def get_setup_recommendations(symptoms_dict: Dict[str, bool]) -> List[Dict[str, Any]]:
    """
    Passes symptoms to the Setup Recommendation Engine and returns rules-based setup advice.
    """
    engine = SetupEngine()
    # Safely convert dict to TelemetrySymptoms model
    symptoms = TelemetrySymptoms(**symptoms_dict)
    recommendations = engine.evaluate(symptoms)
    return [rec.model_dump() for rec in recommendations]

# Define the tools mapping for the agent engine
AGENT_TOOLS = {
    "get_last_session": get_last_session,
    "get_lap_summary": get_lap_summary,
    "compare_laps": compare_laps,
    "get_driver_consistency": get_driver_consistency,
    "get_setup_recommendations": get_setup_recommendations,
}
