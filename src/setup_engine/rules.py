from typing import Callable, List, Dict, Any
from .models import TelemetrySymptoms, SetupRecommendation

class SetupRule:
    def __init__(
        self,
        condition: Callable[[TelemetrySymptoms], bool],
        issue: str,
        recommendation: str,
        why: str,
        tradeoff: str = None,
        confidence: float = 0.8
    ):
        self.condition = condition
        self.issue = issue
        self.recommendation = recommendation
        self.why = why
        self.tradeoff = tradeoff
        self.confidence = confidence

    def evaluate(self, symptoms: TelemetrySymptoms) -> SetupRecommendation | None:
        if self.condition(symptoms):
            return SetupRecommendation(
                issue=self.issue,
                recommendation=self.recommendation,
                why=self.why,
                tradeoff=self.tradeoff,
                confidence=self.confidence
            )
        return None

# ACC-Specific Rules Knowledge Base
RULES_DB: List[SetupRule] = [
    SetupRule(
        condition=lambda s: s.mid_corner_understeer,
        issue="Mid-corner understeer",
        recommendation="Soften front ARB by 1 click",
        why="Increases front mechanical grip during rotation by allowing more roll at the front axle.",
        tradeoff="May reduce response in fast direction changes.",
        confidence=0.85
    ),
    SetupRule(
        condition=lambda s: s.rear_instability_braking,
        issue="Rear instability on braking",
        recommendation="Move brake bias forward by 0.5% - 1.0%",
        why="Shifts braking force away from the rear tyres, preventing them from locking or breaking traction while unloaded.",
        tradeoff="Can increase understeer on corner entry and lock front tyres easier.",
        confidence=0.90
    ),
    SetupRule(
        condition=lambda s: s.exit_wheelspin,
        issue="Exit wheelspin / Traction loss",
        recommendation="Increase TC by 1, or lower differential preload",
        why="TC limits slip directly. Lower diff preload allows the inside wheel to slip without snapping the car sideways.",
        tradeoff="Higher TC cuts power; lower preload reduces corner-exit drive.",
        confidence=0.78
    ),
    SetupRule(
        condition=lambda s: s.front_tyre_temp_high,
        issue="Front tyres overheating",
        recommendation="Decrease front tyre pressure by 0.2 psi, or open front brake ducts",
        why="Lower pressure increases contact patch and dissipates heat. Brake ducts cool the rim.",
        tradeoff="Lower pressure may reduce top speed and steering precision.",
        confidence=0.82
    ),
    SetupRule(
        condition=lambda s: s.high_speed_oversteer,
        issue="High-speed oversteer",
        recommendation="Increase rear wing by 1 click, or increase rear ride height",
        why="Shifts aero balance rearwards, pinning the rear of the car to the track at high speeds.",
        tradeoff="Increases drag, lowering top speed on straights.",
        confidence=0.88
    ),
    SetupRule(
        condition=lambda s: s.bottoming_out,
        issue="Chassis bottoming out",
        recommendation="Increase bump stop rate or increase ride height",
        why="Prevents the chassis from striking the track surface under high aerodynamic load.",
        tradeoff="Higher ride height reduces overall downforce. Stiffer bump stops can make the car snap over bumps.",
        confidence=0.92
    ),
    SetupRule(
        condition=lambda s: s.kerb_instability,
        issue="Instability over kerbs",
        recommendation="Soften fast bump dampers",
        why="Allows the suspension to absorb sharp impacts quickly without upsetting the chassis.",
        tradeoff="May make the car feel slightly disconnected during very aggressive inputs.",
        confidence=0.75
    )
]
