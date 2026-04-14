"""
Sample Data Generator — Generates realistic ACC telemetry data for testing.

Simulates a practice session at Monza with a Ferrari 296 GT3:
  - 10 laps with realistic lap times (~1:47-1:50)
  - Tyre temperature evolution across a stint
  - Fuel depletion
  - Setup changes mid-session
  - One invalid lap (track limits violation)

This allows testing the full pipeline (Kafka, Spark, Analytics, Dashboard)
without needing ACC to be running.

Usage:
    python scripts/generate_sample_data.py
"""

import csv
import json
import math
import os
import random
import sys
from datetime import datetime, timezone, timedelta
from pathlib import Path

# Fix Windows console encoding for emoji output
sys.stdout.reconfigure(encoding="utf-8", errors="replace")

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from config.settings import RAW_DATA_DIR, SAMPLE_DATA_DIR
from src.listener.telemetry_publisher import TELEMETRY_COLUMNS


# ═══════════════════════════════════════════════════════════════
# Simulation Parameters
# ═══════════════════════════════════════════════════════════════

SESSION_CONFIG = {
    "session_id": "acc_20260330_monza_gt3_sample",
    "platform": "ACC",
    "track": "Monza",
    "car": "Ferrari 296 GT3",
    "session_type": "practice",
    "total_laps": 10,
    "polling_rate_hz": 60,
    "track_length_m": 5793.0,
}

# Realistic lap time range (in seconds)
LAP_TIME_RANGE = (107.0, 110.0)  # 1:47 to 1:50

# Sector split ratios (Monza)
SECTOR_RATIOS = [0.265, 0.370, 0.365]  # S1=28%, S2=37%, S3=36.5%

# Speed profile — approximate speeds at each distance% around Monza
# (distance_pct, speed_kmh) — interpolated between points
SPEED_PROFILE = [
    (0.00, 285),   # Start/finish straight (end)
    (0.04, 290),   # Pre-T1 max speed
    (0.05, 75),    # T1 chicane apex
    (0.07, 120),   # T1 exit
    (0.08, 60),    # T2 chicane apex
    (0.11, 220),   # Post-chicane acceleration
    (0.14, 245),   # Curva Grande entry
    (0.18, 200),   # Curva Grande apex
    (0.24, 280),   # Straight to Roggia
    (0.25, 70),    # Roggia chicane
    (0.28, 200),   # Post-Roggia
    (0.34, 170),   # Lesmo 1 apex
    (0.38, 155),   # Lesmo 2 apex
    (0.42, 240),   # Post-Lesmo straight
    (0.48, 260),   # Serraglio
    (0.55, 270),   # Ascari approach
    (0.59, 155),   # Ascari chicane
    (0.63, 230),   # Post-Ascari
    (0.72, 300),   # Back straight max
    (0.80, 310),   # Parabolica approach max
    (0.82, 125),   # Parabolica braking
    (0.87, 140),   # Parabolica apex
    (0.92, 210),   # Parabolica exit
    (1.00, 285),   # Start/finish
]

# Initial tyre temps
INITIAL_TYRE_TEMPS = {
    "fl": 75.0, "fr": 75.0,
    "rl": 72.0, "rr": 72.0,
}

# Optimal tyre temp (GT3 slick)
OPTIMAL_TYRE_TEMP = 90.0


def interpolate_speed(distance_pct: float) -> float:
    """Interpolate speed from the speed profile."""
    for i in range(len(SPEED_PROFILE) - 1):
        d0, s0 = SPEED_PROFILE[i]
        d1, s1 = SPEED_PROFILE[i + 1]
        if d0 <= distance_pct <= d1:
            t = (distance_pct - d0) / (d1 - d0)
            # Smooth interpolation
            t = t * t * (3 - 2 * t)  # Smoothstep
            return s0 + (s1 - s0) * t
    return SPEED_PROFILE[-1][1]


def generate_g_forces(speed_pct_change: float, steering: float) -> tuple:
    """Generate approximate G-forces from speed change and steering."""
    g_lon = speed_pct_change * 3.0  # Braking/accel
    g_lat = steering * 2.5          # Cornering
    g_vert = 1.0 + random.gauss(0, 0.02)
    return (g_lat, g_lon, g_vert)


def evolve_tyre_temps(temps: dict, lap: int, speed: float, distance_pct: float) -> dict:
    """
    Evolve tyre temperatures across a stint.
    
    Tyres build temp over the first 3 laps, then slowly degrade.
    High-speed sections cool the rears slightly, heavy braking heats fronts.
    """
    # Base temp rises for first 3 laps, then stabilizes + slight rise
    warmup_factor = min(1.0, lap / 3.0)
    degradation = max(0, (lap - 5) * 0.3)  # After lap 5, slight overheat
    
    target_base = {
        "fl": OPTIMAL_TYRE_TEMP + 2.0 + degradation * 1.2,
        "fr": OPTIMAL_TYRE_TEMP + 0.0 + degradation * 0.8,
        "rl": OPTIMAL_TYRE_TEMP - 3.0 + degradation * 0.5,
        "rr": OPTIMAL_TYRE_TEMP - 1.5 + degradation * 0.6,
    }
    
    new_temps = {}
    for corner in ["fl", "fr", "rl", "rr"]:
        current = temps[corner]
        target = target_base[corner] * warmup_factor + INITIAL_TYRE_TEMPS[corner] * (1 - warmup_factor)
        
        # Move toward target with some inertia (0.05 = ~1 second time constant at 60Hz)
        new_temps[corner] = current + (target - current) * 0.05 + random.gauss(0, 0.15)
        
        # Rears run slightly warmer under power (tiny per-sample bias)
        if corner.startswith("r"):
            new_temps[corner] += random.gauss(0.002, 0.001)
    
    return new_temps


def generate_sample_session():
    """Generate a complete sample session and write to CSV/JSONL files."""
    cfg = SESSION_CONFIG
    session_id = cfg["session_id"]
    
    # Create output directory
    output_dir = RAW_DATA_DIR / session_id
    output_dir.mkdir(parents=True, exist_ok=True)
    
    print(f"🏎️  Generating sample session: {session_id}")
    print(f"   Track: {cfg['track']} | Car: {cfg['car']}")
    print(f"   Laps: {cfg['total_laps']} | Rate: {cfg['polling_rate_hz']}Hz")
    print(f"   Output: {output_dir}")
    print()
    
    # Open files
    csv_path = output_dir / "telemetry.csv"
    csv_file = open(csv_path, "w", newline="", encoding="utf-8")
    csv_writer = csv.DictWriter(csv_file, fieldnames=TELEMETRY_COLUMNS)
    csv_writer.writeheader()
    
    events_file = open(output_dir / "session_events.jsonl", "w", encoding="utf-8")
    laps_file = open(output_dir / "lap_completions.jsonl", "w", encoding="utf-8")
    setups_file = open(output_dir / "setup_snapshots.jsonl", "w", encoding="utf-8")
    
    # Session start event
    session_start_time = datetime(2026, 3, 30, 14, 20, 0, tzinfo=timezone.utc)
    events_file.write(json.dumps({
        "event_type": "session_start",
        "session_id": session_id,
        "platform": "ACC",
        "track": cfg["track"],
        "car": cfg["car"],
        "session_type": cfg["session_type"],
        "timestamp": session_start_time.isoformat(),
    }) + "\n")
    
    # Initial setup
    setup = {
        "tc_level": 4,
        "tc_cut": 0,
        "abs_level": 5,
        "engine_map": 1,
        "brake_bias": 58.5,
    }
    setups_file.write(json.dumps({
        "session_id": session_id,
        "car": cfg["car"],
        "track": cfg["track"],
        "timestamp": session_start_time.isoformat(),
        "lap_applied_from": 0,
        **setup,
    }) + "\n")
    
    # State
    tyre_temps = dict(INITIAL_TYRE_TEMPS)
    tyre_pressures = {"fl": 27.4, "fr": 27.1, "rl": 26.8, "rr": 26.9}
    fuel = 50.0
    current_time = session_start_time
    total_rows = 0
    best_lap_ms = 999999
    
    # Generate lap-by-lap
    for lap in range(cfg["total_laps"]):
        # Generate lap time with some variance
        base_time = random.uniform(*LAP_TIME_RANGE)
        
        # Faster in middle of stint (tyres warm, fuel lighter)
        stint_factor = -0.3 * math.sin(math.pi * lap / cfg["total_laps"])
        
        # Tyre degradation penalty after lap 7
        deg_penalty = max(0, (lap - 7) * 0.15)
        
        lap_time_s = base_time + stint_factor + deg_penalty + random.gauss(0, 0.2)
        
        # One invalid lap (lap 6 — ran wide at Parabolica)
        is_valid = True
        if lap == 5:
            is_valid = False
            lap_time_s += 1.5  # Track limits adds time
        
        lap_time_ms = int(lap_time_s * 1000)
        
        # Sector times
        sector_times_ms = [
            int(lap_time_ms * ratio + random.gauss(0, 100))
            for ratio in SECTOR_RATIOS
        ]
        # Adjust to make sure they sum correctly
        sector_times_ms[2] = lap_time_ms - sector_times_ms[0] - sector_times_ms[1]
        
        # Fuel consumption
        fuel_used = 2.6 + random.gauss(0, 0.1)
        fuel -= fuel_used
        
        if lap_time_ms < best_lap_ms and is_valid:
            best_lap_ms = lap_time_ms
        
        # Generate 60Hz data points for this lap
        dt = 1.0 / cfg["polling_rate_hz"]
        points_per_lap = int(lap_time_s * cfg["polling_rate_hz"])
        
        prev_speed = interpolate_speed(0.0)
        
        # Mid-session setup change (after lap 4)
        if lap == 4:
            old_tc = setup["tc_level"]
            setup["tc_level"] = 3
            setup["brake_bias"] = 57.0
            setups_file.write(json.dumps({
                "session_id": session_id,
                "car": cfg["car"],
                "track": cfg["track"],
                "timestamp": current_time.isoformat(),
                "lap_applied_from": lap + 1,
                **setup,
            }) + "\n")
            print(f"  🔧 Setup change at lap {lap + 1}: TC {old_tc}→{setup['tc_level']}, BB→{setup['brake_bias']}")
        
        for i in range(points_per_lap):
            distance_pct = i / points_per_lap
            distance_m = distance_pct * cfg["track_length_m"]
            
            # Determine current sector
            sector_boundary_pcts = [0.0]
            acc = 0.0
            for ratio in SECTOR_RATIOS:
                acc += ratio
                sector_boundary_pcts.append(acc)
            
            current_sector = 0
            for s in range(len(SECTOR_RATIOS)):
                if distance_pct >= sector_boundary_pcts[s]:
                    current_sector = s
            
            # Speed and inputs
            speed = interpolate_speed(distance_pct) + random.gauss(0, 2.0)
            speed = max(40, speed)
            
            speed_change = (speed - prev_speed) / max(speed, 1.0)
            prev_speed = speed
            
            # Simulate inputs from speed profile
            braking = max(0, -speed_change * 8.0) + random.gauss(0, 0.02)
            braking = max(0, min(1, braking))
            
            throttle = max(0, speed_change * 5.0 + 0.3) if braking < 0.1 else 0.0
            throttle = max(0, min(1, throttle + random.gauss(0, 0.03)))
            
            # Steering — non-zero in corners
            steering = 0.0
            for (d0, _), (d1, s1) in zip(SPEED_PROFILE, SPEED_PROFILE[1:]):
                if d0 <= distance_pct <= d1 and s1 < 180:
                    steering = random.gauss(0, 0.15)
                    if s1 < 100:
                        steering = random.gauss(0, 0.35)
                    break
            
            # G-forces
            g_lat, g_lon, g_vert = generate_g_forces(speed_change, steering)
            
            # RPM from speed (approximate)
            gear = max(2, min(7, int(speed / 45) + 2))
            rpm = int(3000 + (speed / 310) * 5200 + random.gauss(0, 100))
            
            # Evolve tyres
            tyre_temps = evolve_tyre_temps(tyre_temps, lap, speed, distance_pct)
            
            # Tyre wear (slow degradation)
            wear_base = 1.0 - (lap * 0.005 + i / points_per_lap * 0.005)
            
            # Build row
            row = {
                "timestamp_wall": current_time.isoformat(),
                "timestamp_game": f"{(current_time - session_start_time).total_seconds():.3f}",
                "car": cfg["car"],
                "track": cfg["track"],
                "session_type": cfg["session_type"],
                "status": "live",
                
                "completed_laps": lap,
                "current_lap_time_ms": int(distance_pct * lap_time_ms),
                "last_lap_time_ms": lap_time_ms if lap > 0 else 0,
                "best_lap_time_ms": best_lap_ms,
                "distance_into_lap": f"{distance_m:.2f}",
                "normalized_position": f"{distance_pct:.6f}",
                "current_sector": current_sector,
                "last_sector_time_ms": sector_times_ms[current_sector] if lap > 0 else 0,
                "is_valid_lap": is_valid,
                "is_in_pit": False,
                "is_in_pit_lane": False,
                
                "throttle": f"{throttle:.4f}",
                "brake": f"{braking:.4f}",
                "steering": f"{steering:.4f}",
                "clutch": "0.0000",
                
                "speed_kmh": f"{speed:.2f}",
                "gear": gear,
                "rpm": rpm,
                "fuel_remaining": f"{fuel:.2f}",
                "fuel_per_lap": f"{fuel_used:.2f}",
                
                "g_lat": f"{g_lat:.4f}",
                "g_lon": f"{g_lon:.4f}",
                "g_vert": f"{g_vert:.4f}",
                
                "tyre_pressure_fl": f"{tyre_pressures['fl'] + random.gauss(0, 0.05):.2f}",
                "tyre_pressure_fr": f"{tyre_pressures['fr'] + random.gauss(0, 0.05):.2f}",
                "tyre_pressure_rl": f"{tyre_pressures['rl'] + random.gauss(0, 0.05):.2f}",
                "tyre_pressure_rr": f"{tyre_pressures['rr'] + random.gauss(0, 0.05):.2f}",
                "tyre_temp_fl": f"{tyre_temps['fl']:.1f}",
                "tyre_temp_fr": f"{tyre_temps['fr']:.1f}",
                "tyre_temp_rl": f"{tyre_temps['rl']:.1f}",
                "tyre_temp_rr": f"{tyre_temps['rr']:.1f}",
                "tyre_inner_temp_fl": f"{tyre_temps['fl'] + random.gauss(1.5, 0.5):.1f}",
                "tyre_inner_temp_fr": f"{tyre_temps['fr'] + random.gauss(1.5, 0.5):.1f}",
                "tyre_inner_temp_rl": f"{tyre_temps['rl'] + random.gauss(1.0, 0.3):.1f}",
                "tyre_inner_temp_rr": f"{tyre_temps['rr'] + random.gauss(1.0, 0.3):.1f}",
                "tyre_middle_temp_fl": f"{tyre_temps['fl'] + random.gauss(0.5, 0.3):.1f}",
                "tyre_middle_temp_fr": f"{tyre_temps['fr'] + random.gauss(0.5, 0.3):.1f}",
                "tyre_middle_temp_rl": f"{tyre_temps['rl'] + random.gauss(0.3, 0.2):.1f}",
                "tyre_middle_temp_rr": f"{tyre_temps['rr'] + random.gauss(0.3, 0.2):.1f}",
                "tyre_outer_temp_fl": f"{tyre_temps['fl'] + random.gauss(-1.0, 0.5):.1f}",
                "tyre_outer_temp_fr": f"{tyre_temps['fr'] + random.gauss(-1.0, 0.5):.1f}",
                "tyre_outer_temp_rl": f"{tyre_temps['rl'] + random.gauss(-0.5, 0.3):.1f}",
                "tyre_outer_temp_rr": f"{tyre_temps['rr'] + random.gauss(-0.5, 0.3):.1f}",
                "tyre_slip_fl": f"{abs(random.gauss(0, 0.02 + braking * 0.05)):.4f}",
                "tyre_slip_fr": f"{abs(random.gauss(0, 0.02 + braking * 0.05)):.4f}",
                "tyre_slip_rl": f"{abs(random.gauss(0, 0.01 + throttle * 0.03)):.4f}",
                "tyre_slip_rr": f"{abs(random.gauss(0, 0.01 + throttle * 0.03)):.4f}",
                "suspension_travel_fl": f"{0.035 + random.gauss(0, 0.005):.4f}",
                "suspension_travel_fr": f"{0.033 + random.gauss(0, 0.005):.4f}",
                "suspension_travel_rl": f"{0.040 + random.gauss(0, 0.005):.4f}",
                "suspension_travel_rr": f"{0.038 + random.gauss(0, 0.005):.4f}",
                "brake_temp_fl": f"{350 + braking * 200 + random.gauss(0, 10):.1f}",
                "brake_temp_fr": f"{345 + braking * 195 + random.gauss(0, 10):.1f}",
                "brake_temp_rl": f"{280 + braking * 120 + random.gauss(0, 8):.1f}",
                "brake_temp_rr": f"{275 + braking * 115 + random.gauss(0, 8):.1f}",
                "tyre_wear_fl": f"{wear_base - 0.005 + random.gauss(0, 0.001):.4f}",
                "tyre_wear_fr": f"{wear_base - 0.003 + random.gauss(0, 0.001):.4f}",
                "tyre_wear_rl": f"{wear_base + 0.002 + random.gauss(0, 0.001):.4f}",
                "tyre_wear_rr": f"{wear_base + random.gauss(0, 0.001):.4f}",
                
                "tc_level": setup["tc_level"],
                "tc_cut": setup["tc_cut"],
                "abs_level": setup["abs_level"],
                "engine_map": setup["engine_map"],
                "brake_bias": f"{setup['brake_bias']:.1f}",
                
                "air_temp": f"{24.0 + random.gauss(0, 0.1):.1f}",
                "road_temp": f"{32.0 + random.gauss(0, 0.2):.1f}",
                "track_grip": "optimum",
                "rain_intensity": "no_rain",
                "wind_speed": f"{5.0 + random.gauss(0, 0.5):.1f}",
                "wind_direction": f"{180.0 + random.gauss(0, 5):.1f}",
            }
            
            csv_writer.writerow(row)
            total_rows += 1
            current_time += timedelta(seconds=dt)
        
        # Lap completion event
        laps_file.write(json.dumps({
            "session_id": session_id,
            "lap_number": lap + 1,
            "lap_time_ms": lap_time_ms,
            "sector_1_ms": sector_times_ms[0],
            "sector_2_ms": sector_times_ms[1],
            "sector_3_ms": sector_times_ms[2],
            "is_valid": is_valid,
            "fuel_used": round(fuel_used, 2),
            "fuel_remaining": round(fuel, 2),
            "avg_tyre_temp_fl": round(tyre_temps["fl"], 1),
            "avg_tyre_temp_fr": round(tyre_temps["fr"], 1),
            "avg_tyre_temp_rl": round(tyre_temps["rl"], 1),
            "avg_tyre_temp_rr": round(tyre_temps["rr"], 1),
            "timestamp": current_time.isoformat(),
        }) + "\n")
        
        valid_str = "✅" if is_valid else "❌"
        print(f"  Lap {lap + 1:2d}: {lap_time_ms/1000:.3f}s "
              f"[S1: {sector_times_ms[0]/1000:.3f} | S2: {sector_times_ms[1]/1000:.3f} | S3: {sector_times_ms[2]/1000:.3f}] "
              f"{valid_str} | Fuel: {fuel:.1f}L")
    
    # Session end event
    events_file.write(json.dumps({
        "event_type": "session_end",
        "session_id": session_id,
        "platform": "ACC",
        "track": cfg["track"],
        "car": cfg["car"],
        "session_type": cfg["session_type"],
        "timestamp": current_time.isoformat(),
        "total_laps": cfg["total_laps"],
        "best_lap_ms": best_lap_ms,
    }) + "\n")
    
    # Close files
    csv_file.close()
    events_file.close()
    laps_file.close()
    setups_file.close()
    
    print()
    print(f"✅ Generated {total_rows:,} telemetry rows ({total_rows / cfg['polling_rate_hz']:.0f}s of data)")
    print(f"   Best lap: {best_lap_ms/1000:.3f}s")
    print(f"   Output: {output_dir}")
    
    # Print file sizes
    for f in output_dir.iterdir():
        size_kb = f.stat().st_size / 1024
        print(f"   📄 {f.name}: {size_kb:.1f} KB")


if __name__ == "__main__":
    generate_sample_session()
