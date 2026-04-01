"""
Schema Validation Tests — Verify telemetry data quality.

Tests:
  1. All expected columns present in CSV output
  2. Data types are correct (no garbled values from misaligned structs)
  3. Lap times are in realistic range
  4. Distance into lap resets at lap boundaries
  5. Fuel depletes monotonically within a lap
  6. Tyre temperatures are in physically plausible range
"""

import csv
import json
import os
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))

from src.listener.telemetry_publisher import TELEMETRY_COLUMNS
from config.settings import RAW_DATA_DIR


# ═══════════════════════════════════════════════════════════════
# Fixtures
# ═══════════════════════════════════════════════════════════════

SAMPLE_SESSION = "acc_20260330_monza_gt3_sample"
SAMPLE_DIR = RAW_DATA_DIR / SAMPLE_SESSION


@pytest.fixture(scope="module")
def telemetry_rows():
    """Load all telemetry rows from the sample session."""
    csv_path = SAMPLE_DIR / "telemetry.csv"
    if not csv_path.exists():
        pytest.skip(f"Sample data not found at {csv_path}. Run generate_sample_data.py first.")
    
    with open(csv_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        return list(reader)


@pytest.fixture(scope="module")
def lap_completions():
    """Load all lap completion events from the sample session."""
    path = SAMPLE_DIR / "lap_completions.jsonl"
    if not path.exists():
        pytest.skip("Sample data not found. Run generate_sample_data.py first.")
    
    with open(path, encoding="utf-8") as f:
        return [json.loads(line) for line in f if line.strip()]


@pytest.fixture(scope="module")
def session_events():
    """Load session events from the sample session."""
    path = SAMPLE_DIR / "session_events.jsonl"
    if not path.exists():
        pytest.skip("Sample data not found. Run generate_sample_data.py first.")
    
    with open(path, encoding="utf-8") as f:
        return [json.loads(line) for line in f if line.strip()]


# ═══════════════════════════════════════════════════════════════
# Column Presence Tests
# ═══════════════════════════════════════════════════════════════

class TestSchemaCompleteness:
    """Verify all expected columns are present in the CSV output."""
    
    def test_all_columns_present(self, telemetry_rows):
        """Every column from TELEMETRY_COLUMNS must exist in the CSV."""
        if not telemetry_rows:
            pytest.skip("No telemetry rows")
        
        actual_columns = set(telemetry_rows[0].keys())
        expected_columns = set(TELEMETRY_COLUMNS)
        
        missing = expected_columns - actual_columns
        extra = actual_columns - expected_columns
        
        assert not missing, f"Missing columns: {missing}"
        # Extra columns are okay (forward compatibility)
    
    def test_no_empty_required_fields(self, telemetry_rows):
        """Critical fields must not be empty."""
        required_fields = [
            "timestamp_wall", "car", "track", "speed_kmh",
            "throttle", "brake", "distance_into_lap",
        ]
        
        for i, row in enumerate(telemetry_rows[:100]):  # Check first 100 rows
            for field in required_fields:
                assert row.get(field, "") != "", f"Row {i}: field '{field}' is empty"


# ═══════════════════════════════════════════════════════════════
# Data Type Tests
# ═══════════════════════════════════════════════════════════════

class TestDataTypes:
    """Verify data values are correctly typed and in plausible ranges."""
    
    def test_speed_is_numeric(self, telemetry_rows):
        """Speed should be a valid float."""
        for row in telemetry_rows[:1000]:
            speed = float(row["speed_kmh"])
            assert 0 <= speed <= 400, f"Speed {speed} out of range"
    
    def test_throttle_brake_in_range(self, telemetry_rows):
        """Throttle and brake should be [0.0, 1.0]."""
        for row in telemetry_rows[:1000]:
            throttle = float(row["throttle"])
            brake = float(row["brake"])
            assert -0.01 <= throttle <= 1.01, f"Throttle {throttle} out of range"
            assert -0.01 <= brake <= 1.01, f"Brake {brake} out of range"
    
    def test_tyre_temps_plausible(self, telemetry_rows):
        """Tyre temps should be between 20°C and 150°C."""
        corners = ["fl", "fr", "rl", "rr"]
        for row in telemetry_rows[:1000]:
            for c in corners:
                temp = float(row[f"tyre_temp_{c}"])
                assert 20 <= temp <= 150, f"Tyre temp {c}={temp}°C out of range"
    
    def test_fuel_positive(self, telemetry_rows):
        """Fuel remaining should be positive."""
        for row in telemetry_rows[:1000]:
            fuel = float(row["fuel_remaining"])
            assert fuel >= 0, f"Negative fuel: {fuel}"
    
    def test_distance_positive(self, telemetry_rows):
        """Distance into lap should be non-negative."""
        for row in telemetry_rows[:1000]:
            dist = float(row["distance_into_lap"])
            assert dist >= 0, f"Negative distance: {dist}"


# ═══════════════════════════════════════════════════════════════
# Lap Validation Tests
# ═══════════════════════════════════════════════════════════════

class TestLapData:
    """Verify lap completion data is consistent."""
    
    def test_lap_times_realistic(self, lap_completions):
        """Lap times should be between 1:30 and 2:30 for Monza GT3."""
        for lap in lap_completions:
            time_ms = lap["lap_time_ms"]
            time_s = time_ms / 1000
            assert 90 <= time_s <= 150, f"Lap {lap['lap_number']}: {time_s}s unrealistic"
    
    def test_sector_times_sum_to_lap(self, lap_completions):
        """Sum of sector times should approximately equal lap time."""
        for lap in lap_completions:
            sector_sum = lap["sector_1_ms"] + lap["sector_2_ms"] + lap["sector_3_ms"]
            lap_time = lap["lap_time_ms"]
            delta = abs(sector_sum - lap_time)
            # Allow up to 100ms discrepancy (normal in game timing)
            assert delta <= 100, (
                f"Lap {lap['lap_number']}: sectors sum {sector_sum}ms ≠ "
                f"lap time {lap_time}ms (delta: {delta}ms)"
            )
    
    def test_at_least_one_invalid_lap(self, lap_completions):
        """Sample data should have at least one invalid lap (testing invalidity handling)."""
        invalid_laps = [l for l in lap_completions if not l["is_valid"]]
        assert len(invalid_laps) >= 1, "Expected at least one invalid lap in sample data"
    
    def test_lap_numbers_sequential(self, lap_completions):
        """Lap numbers should be sequential starting from 1."""
        numbers = [l["lap_number"] for l in lap_completions]
        for i, n in enumerate(numbers):
            assert n == i + 1, f"Expected lap {i+1}, got {n}"
    
    def test_fuel_depletes(self, lap_completions):
        """Fuel should decrease across laps."""
        fuels = [l["fuel_remaining"] for l in lap_completions]
        for i in range(1, len(fuels)):
            assert fuels[i] < fuels[i-1], (
                f"Fuel increased: lap {i} = {fuels[i]}, lap {i+1} = {fuels[i+1]}"
            )


# ═══════════════════════════════════════════════════════════════
# Session Event Tests
# ═══════════════════════════════════════════════════════════════

class TestSessionEvents:
    """Verify session event structure."""
    
    def test_session_start_exists(self, session_events):
        """Should have a session_start event."""
        starts = [e for e in session_events if e["event_type"] == "session_start"]
        assert len(starts) == 1
    
    def test_session_end_exists(self, session_events):
        """Should have a session_end event."""
        ends = [e for e in session_events if e["event_type"] == "session_end"]
        assert len(ends) == 1
    
    def test_session_end_has_lap_count(self, session_events):
        """Session end event should include total laps."""
        end = [e for e in session_events if e["event_type"] == "session_end"][0]
        assert "total_laps" in end
        assert end["total_laps"] > 0
