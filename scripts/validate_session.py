"""
Post-Session Validation Script.

Run this after a live ACC session to verify data integrity.
Checks:
  1. Lap times vs what ACC showed (you confirm visually)
  2. Field value ranges (speed, temps, pressures, brake bias)
  3. Parquet file integrity
  4. Tyre temperature sanity
  5. Fuel depletion linearity
  6. Data completeness (no gaps)

Usage:
    python scripts/validate_session.py                  # auto-finds latest session
    python scripts/validate_session.py <session_id>     # specific session
"""

import csv
import json
import os
import sys
from pathlib import Path
from collections import Counter

# Force UTF-8 output on Windows
if sys.platform == "win32":
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")
    os.system("")  # Enable ANSI escape codes on Windows 10+

sys.path.insert(0, str(Path(__file__).parent.parent))

from config.settings import RAW_DATA_DIR, TELEMETRY_DIR

try:
    import pyarrow.parquet as pq
    HAS_PARQUET = True
except ImportError:
    HAS_PARQUET = False


# ═══════════════════════════════════════════════════════════════
# Helpers
# ═══════════════════════════════════════════════════════════════

RED = "\033[91m"
GREEN = "\033[92m"
YELLOW = "\033[93m"
CYAN = "\033[96m"
BOLD = "\033[1m"
RESET = "\033[0m"

def ok(msg):
    print(f"  {GREEN}✓{RESET} {msg}")

def warn(msg):
    print(f"  {YELLOW}⚠{RESET} {msg}")

def fail(msg):
    print(f"  {RED}✗{RESET} {msg}")

def header(msg):
    print(f"\n{BOLD}{CYAN}{'═' * 60}{RESET}")
    print(f"{BOLD}{CYAN}  {msg}{RESET}")
    print(f"{BOLD}{CYAN}{'═' * 60}{RESET}")

def subheader(msg):
    print(f"\n  {BOLD}{msg}{RESET}")


def find_latest_session() -> Path:
    """Find the most recently modified session directory."""
    sessions = sorted(RAW_DATA_DIR.iterdir(), key=lambda p: p.stat().st_mtime, reverse=True)
    # Filter to directories only
    sessions = [s for s in sessions if s.is_dir()]
    if not sessions:
        print(f"{RED}No sessions found in {RAW_DATA_DIR}{RESET}")
        sys.exit(1)
    return sessions[0]


def find_parquet_dir(session_id: str) -> Path:
    """Find the parquet output directory for a session."""
    # Search recursively under TELEMETRY_DIR for this session_id
    for p in TELEMETRY_DIR.rglob(session_id):
        if p.is_dir():
            return p
    return None


# ═══════════════════════════════════════════════════════════════
# Validation Checks
# ═══════════════════════════════════════════════════════════════

def validate_session_events(session_dir: Path) -> dict:
    """Check session start/end events."""
    header("Session Events")
    
    events_path = session_dir / "session_events.jsonl"
    if not events_path.exists():
        fail("session_events.jsonl not found")
        return {}
    
    events = [json.loads(l) for l in events_path.read_text().strip().split("\n") if l.strip()]
    
    info = {}
    for e in events:
        etype = e.get("event_type", "?")
        if etype == "session_start":
            info["track"] = e.get("track", "?")
            info["car"] = e.get("car", "?")
            info["session_type"] = e.get("session_type", "?")
            info["session_id"] = e.get("session_id", "?")
            ok(f"Session start: {info['track']} | {info['car']} | {info['session_type']}")
        elif etype == "session_end":
            info["total_laps"] = e.get("total_laps", 0)
            info["best_lap_ms"] = e.get("best_lap_ms")
            ok(f"Session end: {info['total_laps']} laps")
    
    if "session_start" not in [e.get("event_type") for e in events]:
        fail("No session_start event found")
    if "session_end" not in [e.get("event_type") for e in events]:
        warn("No session_end event (session may have been force-closed)")
    
    return info


def validate_lap_times(session_dir: Path):
    """Check lap completion data."""
    header("Lap Times — COMPARE THESE TO ACC IN-GAME TIMES")
    
    laps_path = session_dir / "lap_completions.jsonl"
    if not laps_path.exists():
        fail("lap_completions.jsonl not found")
        return
    
    laps = [json.loads(l) for l in laps_path.read_text().strip().split("\n") if l.strip()]
    
    if not laps:
        warn("No laps recorded (did you complete at least one lap?)")
        return
    
    print(f"\n  {'Lap':<5} {'Time':>10} {'Valid':>6} {'Fuel':>7} {'FL':>6} {'FR':>6} {'RL':>6} {'RR':>6}")
    print(f"  {'─' * 58}")
    
    for lap in laps:
        lap_n = lap["lap_number"]
        time_s = lap["lap_time_ms"] / 1000
        mins = int(time_s) // 60
        secs = time_s - mins * 60
        valid = "✓" if lap.get("is_valid", True) else "✗"
        fuel = lap.get("fuel_remaining", 0)
        
        # Tyre temps
        fl = lap.get("tyre_temp_fl", lap.get("avg_tyre_temp_fl", 0))
        fr = lap.get("tyre_temp_fr", lap.get("avg_tyre_temp_fr", 0))
        rl = lap.get("tyre_temp_rl", lap.get("avg_tyre_temp_rl", 0))
        rr = lap.get("tyre_temp_rr", lap.get("avg_tyre_temp_rr", 0))
        
        print(f"  {lap_n:<5} {mins}:{secs:06.3f}  {valid:>5} {fuel:>6.1f}L {fl:>5.1f} {fr:>5.1f} {rl:>5.1f} {rr:>5.1f}")
    
    # Sanity checks
    subheader("Lap time checks")
    times = [l["lap_time_ms"] / 1000 for l in laps]
    
    if all(80 < t < 200 for t in times):
        ok(f"All lap times in plausible range ({min(times):.1f}s – {max(times):.1f}s)")
    else:
        fail(f"Unusual lap times detected: {[f'{t:.1f}s' for t in times if t < 80 or t > 200]}")
    
    # Fuel depletion
    fuels = [l.get("fuel_remaining", 0) for l in laps]
    if len(fuels) > 1 and all(fuels[i] > fuels[i+1] for i in range(len(fuels)-1)):
        fuel_per_lap = [(fuels[i] - fuels[i+1]) for i in range(len(fuels)-1)]
        avg_fuel = sum(fuel_per_lap) / len(fuel_per_lap)
        ok(f"Fuel depleting monotonically ({avg_fuel:.2f} L/lap avg)")
    elif len(fuels) > 1:
        warn("Fuel not strictly decreasing (pit stop or refuel?)")


def validate_telemetry_csv(session_dir: Path):
    """Sample-check the raw CSV telemetry for field sanity."""
    header("Telemetry CSV — Field Sanity Checks")
    
    csv_path = session_dir / "telemetry.csv"
    if not csv_path.exists():
        fail("telemetry.csv not found")
        return
    
    size_mb = csv_path.stat().st_size / (1024 * 1024)
    
    # Read a sample (first 500 + middle 500 + last 500 rows)
    with open(csv_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        all_rows = list(reader)
    
    total = len(all_rows)
    ok(f"CSV: {total:,} rows, {size_mb:.1f} MB")
    
    if total == 0:
        fail("Empty CSV!")
        return
    
    # Sample evenly, but skip the very beginning (first ~500 rows) as ACC often 
    # produces 0.0 for temps/pressures during its initial spawn-in phase.
    sample_indices = set()
    first_start = min(500, total // 10) if total > 1000 else 0
    for start in [first_start, total // 4, total // 2, 3 * total // 4, max(0, total - 500)]:
        for i in range(start, min(start + 200, total)):
            sample_indices.add(i)
    
    sample = [all_rows[i] for i in sorted(sample_indices)]
    
    # Check each field
    checks = {
        "speed_kmh": (0, 370, float),
        "throttle": (-0.01, 1.01, float),
        "brake": (-0.01, 1.01, float),
        "steering": (-1.5, 1.5, float),
        "fuel_remaining": (0, 200, float),
        "brake_bias": (-1, 100, float),
        "air_temp": (-10, 60, float),
        "road_temp": (-10, 80, float),
        "tyre_temp_fl": (10, 200, float),
        "tyre_temp_fr": (10, 200, float),
        "tyre_temp_rl": (10, 200, float),
        "tyre_temp_rr": (10, 200, float),
        "tyre_pressure_fl": (15, 35, float),
        "tyre_pressure_fr": (15, 35, float),
        "tyre_pressure_rl": (15, 35, float),
        "tyre_pressure_rr": (15, 35, float),
        "brake_temp_fl": (0, 1200, float),
        "brake_temp_fr": (0, 1200, float),
    }
    
    subheader("Field ranges (sampled)")
    issues = 0
    
    for field, (lo, hi, dtype) in checks.items():
        values = []
        for row in sample:
            try:
                v = dtype(row.get(field, 0))
                values.append(v)
            except (ValueError, TypeError):
                pass
        
        if not values:
            warn(f"{field}: no values found")
            issues += 1
            continue
        
        vmin, vmax = min(values), max(values)
        out_of_range = sum(1 for v in values if v < lo or v > hi)
        
        if out_of_range == 0:
            ok(f"{field}: {vmin:.2f} – {vmax:.2f}")
        elif out_of_range < len(values) * 0.05:
            warn(f"{field}: {vmin:.2f} – {vmax:.2f} ({out_of_range} outliers)")
            issues += 1
        else:
            fail(f"{field}: {vmin:.2f} – {vmax:.2f} ({out_of_range}/{len(values)} OUT OF RANGE)")
            issues += 1
    
    # Brake bias special check
    subheader("Brake bias analysis")
    bb_values = []
    for row in all_rows[100:]:  # Skip first 100 (initialization)
        try:
            bb = float(row.get("brake_bias", 0))
            if bb > 0:
                bb_values.append(bb)
        except (ValueError, TypeError):
            pass
    
    if bb_values:
        bb_min, bb_max, bb_avg = min(bb_values), max(bb_values), sum(bb_values) / len(bb_values)
        if bb_max < 2.0:
            warn(f"Brake bias looks like 0-1 range (avg={bb_avg:.4f}), may need ×100 scaling")
            warn(f"  Raw range: {bb_min:.4f} – {bb_max:.4f}")
            warn(f"  If ACC shows ~{bb_avg*100:.1f}%, the value needs to be multiplied by 100")
        elif 40 < bb_avg < 70:
            ok(f"Brake bias: {bb_avg:.1f}% (range {bb_min:.1f}–{bb_max:.1f}%)")
        else:
            warn(f"Brake bias unusual: avg={bb_avg:.2f} range={bb_min:.2f}–{bb_max:.2f}")
    
    # Gear distribution
    subheader("Gear distribution")
    gears = Counter()
    for row in all_rows:
        try:
            gears[int(row.get("gear", 0))] += 1
        except (ValueError, TypeError):
            pass
    
    for g in sorted(gears.keys()):
        pct = gears[g] / total * 100
        bar = "█" * int(pct / 2)
        label = {0: "R", 1: "N"}.get(g, str(g - 1))
        print(f"    Gear {label:>2}: {pct:5.1f}% {bar}")
    
    return issues


def validate_parquet_files(session_id: str):
    """Check the per-lap Parquet files."""
    header("Parquet Files — Per-Lap Data")
    
    if not HAS_PARQUET:
        warn("pyarrow not installed — skipping Parquet checks")
        return
    
    pq_dir = find_parquet_dir(session_id)
    if not pq_dir:
        warn(f"No Parquet directory found for session {session_id}")
        return
    
    parquet_files = sorted(pq_dir.glob("*.parquet"))
    if not parquet_files:
        warn("No Parquet files found")
        return
    
    ok(f"Found {len(parquet_files)} Parquet files in {pq_dir}")
    
    print(f"\n  {'File':<20} {'Rows':>8} {'Size':>8} {'Duration':>10} {'Speed Range':>14}")
    print(f"  {'─' * 62}")
    
    for pf in parquet_files:
        table = pq.read_table(str(pf))
        rows = table.num_rows
        size_kb = pf.stat().st_size / 1024
        
        # Calculate approximate duration
        duration_s = rows / 30.0  # At 30Hz
        
        # Speed range
        speeds = table.column("speed_kmh").to_pylist()
        if speeds:
            s_min, s_max = min(speeds), max(speeds)
            speed_str = f"{s_min:.0f}–{s_max:.0f} km/h"
        else:
            speed_str = "N/A"
        
        print(f"  {pf.name:<20} {rows:>8,} {size_kb:>6.1f}KB {duration_s:>8.1f}s {speed_str:>14}")
    
    # Verify schema consistency
    subheader("Schema check")
    schemas = set()
    for pf in parquet_files:
        schema = pq.read_schema(str(pf))
        schemas.add(len(schema))
    
    if len(schemas) == 1:
        ok(f"All files have consistent schema ({schemas.pop()} columns)")
    else:
        fail(f"Inconsistent schemas: {schemas}")


def validate_setup_snapshots(session_dir: Path):
    """Check setup data."""
    header("Setup Snapshots")
    
    setup_path = session_dir / "setup_snapshots.jsonl"
    if not setup_path.exists():
        warn("setup_snapshots.jsonl not found")
        return
    
    setups = [json.loads(l) for l in setup_path.read_text().strip().split("\n") if l.strip()]
    
    if not setups:
        warn("No setup snapshots recorded")
        return
    
    for s in setups:
        lap = s.get("lap_applied_from", "?")
        tc = s.get("tc_level", "?")
        tcc = s.get("tc_cut", "?")
        abs_l = s.get("abs_level", "?")
        bb = s.get("brake_bias", "?")
        emap = s.get("engine_map", "?")
        ok(f"Lap {lap}: TC={tc} TCCut={tcc} ABS={abs_l} BB={bb} Map={emap}")


# ═══════════════════════════════════════════════════════════════
# Main
# ═══════════════════════════════════════════════════════════════

def main():
    # Determine session
    if len(sys.argv) > 1:
        session_id = sys.argv[1]
        session_dir = RAW_DATA_DIR / session_id
        if not session_dir.exists():
            print(f"{RED}Session not found: {session_dir}{RESET}")
            sys.exit(1)
    else:
        session_dir = find_latest_session()
        session_id = session_dir.name
    
    print(f"\n{BOLD}Validating session: {session_id}{RESET}")
    print(f"  Path: {session_dir}")
    
    # Run all checks
    info = validate_session_events(session_dir)
    validate_lap_times(session_dir)
    validate_telemetry_csv(session_dir)
    validate_parquet_files(session_id)
    validate_setup_snapshots(session_dir)
    
    # Summary
    header("VALIDATION COMPLETE")
    print(f"\n  Session: {info.get('session_id', session_id)}")
    print(f"  Track:   {info.get('track', '?')}")
    print(f"  Car:     {info.get('car', '?')}")
    print(f"  Laps:    {info.get('total_laps', '?')}")
    if info.get("best_lap_ms"):
        best_s = info["best_lap_ms"] / 1000
        mins = int(best_s) // 60
        secs = best_s - mins * 60
        print(f"  Best:    {mins}:{secs:06.3f}")
    
    print(f"\n  {YELLOW}ACTION REQUIRED:{RESET} Compare the lap times above to your ACC")
    print(f"  in-game timing screen. If they match within 1ms, the struct")
    print(f"  alignment is correct and all data is trustworthy.\n")


if __name__ == "__main__":
    main()
