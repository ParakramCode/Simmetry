from typing import List, Optional
from pydantic import BaseModel, Field

class TelemetrySymptoms(BaseModel):
    """
    Standardized telemetry symptoms passed to the setup engine.
    Booleans indicate presence of a symptom.
    """
    mid_corner_understeer: bool = False
    entry_understeer: bool = False
    exit_understeer: bool = False
    
    mid_corner_oversteer: bool = False
    entry_oversteer: bool = False
    exit_oversteer: bool = False
    
    rear_instability_braking: bool = False
    exit_wheelspin: bool = False
    high_speed_oversteer: bool = False
    
    front_tyre_temp_high: bool = False
    rear_tyre_temp_high: bool = False
    tyre_pressure_uneven: bool = False
    
    bottoming_out: bool = False
    kerb_instability: bool = False

class SetupRecommendation(BaseModel):
    """
    Explainable recommendation returned by the engine.
    """
    issue: str = Field(..., description="The symptom being addressed")
    recommendation: str = Field(..., description="Actionable setup change")
    why: str = Field(..., description="Engineering explanation of why this works")
    tradeoff: Optional[str] = Field(None, description="Potential negative side effects")
    confidence: float = Field(..., ge=0.0, le=1.0, description="Confidence score (0 to 1)")
    
class SetupResponse(BaseModel):
    recommendations: List[SetupRecommendation]
