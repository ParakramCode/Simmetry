from typing import List
from .models import TelemetrySymptoms, SetupRecommendation
from .rules import RULES_DB

class SetupEngine:
    """
    Rules-based engine for converting telemetry symptoms into actionable
    ACC setup recommendations.
    """
    def __init__(self):
        self.rules = RULES_DB

    def evaluate(self, symptoms: TelemetrySymptoms) -> List[SetupRecommendation]:
        """
        Evaluate all registered rules against the provided symptoms.
        Returns a sorted list of recommendations (highest confidence first).
        """
        recommendations = []
        for rule in self.rules:
            rec = rule.evaluate(symptoms)
            if rec:
                recommendations.append(rec)
        
        # Sort by confidence descending
        recommendations.sort(key=lambda r: r.confidence, reverse=True)
        return recommendations
