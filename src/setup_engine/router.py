from fastapi import APIRouter
from .models import TelemetrySymptoms, SetupResponse
from .engine import SetupEngine

router = APIRouter(prefix="/setup", tags=["Setup Recommendation"])
engine = SetupEngine()

@router.post("/recommend", response_model=SetupResponse)
async def get_setup_recommendation(symptoms: TelemetrySymptoms):
    """
    Submit telemetry symptoms to get an explainable list of 
    setup recommendations ranked by confidence.
    """
    recommendations = engine.evaluate(symptoms)
    return SetupResponse(recommendations=recommendations)
