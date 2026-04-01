"""
Session Manager — Detects session lifecycle events from ACC telemetry.

Responsibilities:
  1. Detect session start (transition from OFF/PAUSE → LIVE)
  2. Detect session end (transition from LIVE → OFF/PAUSE)
  3. Detect setup changes mid-session (TC, ABS, brake bias, etc.)
  4. Track lap completions and sector times
  5. Generate session IDs

The session manager is stateful — it tracks the previous snapshot
to detect transitions. It produces events that are published to
Kafka (Week 2+) or written to session log files (Week 1).
"""

import json
from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from typing import Optional, List, Dict, Any, Callable
from loguru import logger

from .acc_reader import ACCTelemetrySnapshot
from config.settings import generate_session_id


# ═══════════════════════════════════════════════════════════════
# Event Types
# ═══════════════════════════════════════════════════════════════

@dataclass
class SessionEvent:
    """Emitted when a session starts or ends."""
    event_type: str                # "session_start" | "session_end"
    session_id: str
    platform: str = "ACC"
    track: str = ""
    car: str = ""
    session_type: str = ""        # "practice", "qualify", "race"
    timestamp: str = ""
    total_laps: Optional[int] = None
    best_lap_ms: Optional[int] = None
    
    def to_dict(self) -> dict:
        return {k: v for k, v in asdict(self).items() if v is not None}


@dataclass
class LapCompletionEvent:
    """Emitted when a lap boundary is detected."""
    session_id: str
    lap_number: int
    lap_time_ms: int
    last_sector_time_ms: int
    is_valid: bool
    fuel_remaining: float
    timestamp: str = ""
    
    # Tyre data at lap completion
    tyre_temp_fl: float = 0.0
    tyre_temp_fr: float = 0.0
    tyre_temp_rl: float = 0.0
    tyre_temp_rr: float = 0.0
    tyre_pressure_fl: float = 0.0
    tyre_pressure_fr: float = 0.0
    tyre_pressure_rl: float = 0.0
    tyre_pressure_rr: float = 0.0
    
    def to_dict(self) -> dict:
        return asdict(self)


@dataclass
class SetupChangeEvent:
    """Emitted when a car setup parameter changes during a session."""
    session_id: str
    parameter: str               # e.g., "tc_level", "abs_level", "brake_bias"
    old_value: Any
    new_value: Any
    lap_number: int
    timestamp: str = ""
    
    def to_dict(self) -> dict:
        return asdict(self)


@dataclass
class SetupSnapshot:
    """Full setup state at a point in time."""
    session_id: str
    car: str
    track: str
    timestamp: str
    lap_applied_from: int
    tc_level: int
    tc_cut: int
    abs_level: int
    engine_map: int
    brake_bias: float
    
    def to_dict(self) -> dict:
        return asdict(self)


# ═══════════════════════════════════════════════════════════════
# Session Manager
# ═══════════════════════════════════════════════════════════════

class SessionManager:
    """
    Tracks session lifecycle and detects events from telemetry snapshots.
    
    Feed it consecutive ACCTelemetrySnapshot objects; it will emit events
    through registered callbacks.
    
    Usage:
        manager = SessionManager()
        manager.on_session_start(lambda e: print(f"Session started: {e.session_id}"))
        manager.on_lap_completed(lambda e: print(f"Lap {e.lap_number}: {e.lap_time_ms}ms"))
        
        while running:
            snapshot = reader.read()
            if snapshot:
                manager.update(snapshot)
    """
    
    # Setup parameters we track for changes
    TRACKED_SETUP_PARAMS = [
        "tc_level", "tc_cut", "abs_level", "engine_map", "brake_bias"
    ]
    
    def __init__(self):
        self._current_session_id: Optional[str] = None
        self._previous_snapshot: Optional[ACCTelemetrySnapshot] = None
        self._session_active = False
        
        # Track setup state for change detection
        self._current_setup: Dict[str, Any] = {}
        
        # Track lap state
        self._last_completed_laps: int = -1
        self._session_start_time: Optional[datetime] = None
        
        # Event callbacks
        self._session_start_callbacks: List[Callable] = []
        self._session_end_callbacks: List[Callable] = []
        self._lap_completed_callbacks: List[Callable] = []
        self._setup_change_callbacks: List[Callable] = []
        self._setup_snapshot_callbacks: List[Callable] = []
        
        logger.info("SessionManager initialized")
    
    # ── Callback registration ──
    
    def on_session_start(self, callback: Callable[[SessionEvent], None]):
        self._session_start_callbacks.append(callback)
    
    def on_session_end(self, callback: Callable[[SessionEvent], None]):
        self._session_end_callbacks.append(callback)
    
    def on_lap_completed(self, callback: Callable[[LapCompletionEvent], None]):
        self._lap_completed_callbacks.append(callback)
    
    def on_setup_change(self, callback: Callable[[SetupChangeEvent], None]):
        self._setup_change_callbacks.append(callback)
    
    def on_setup_snapshot(self, callback: Callable[[SetupSnapshot], None]):
        self._setup_snapshot_callbacks.append(callback)
    
    # ── Event emission ──
    
    def _emit(self, callbacks: List[Callable], event):
        for cb in callbacks:
            try:
                cb(event)
            except Exception as e:
                logger.error(f"Error in event callback: {e}")
    
    # ── Core update loop ──
    
    def update(self, snapshot: ACCTelemetrySnapshot):
        """
        Process a new telemetry snapshot. Detects:
          - Session start/end transitions
          - Lap completions
          - Setup parameter changes
        """
        prev = self._previous_snapshot
        
        # ── Session start detection ──
        if not self._session_active and snapshot.status == "live":
            self._start_session(snapshot)
        
        # ── Session end detection ──
        elif self._session_active and snapshot.status in ("off", "pause"):
            self._end_session(snapshot)
        
        # ── During active session ──
        if self._session_active and snapshot.status == "live":
            # Check for lap completion
            if prev and snapshot.completed_laps > self._last_completed_laps and self._last_completed_laps >= 0:
                self._handle_lap_completion(snapshot)
            self._last_completed_laps = snapshot.completed_laps
            
            # Check for setup changes
            if prev:
                self._check_setup_changes(snapshot)
        
        self._previous_snapshot = snapshot
    
    def _start_session(self, snapshot: ACCTelemetrySnapshot):
        """Handle session start."""
        car_class = "gt3"  # Default — could be refined based on car model
        self._current_session_id = generate_session_id("acc", snapshot.track, car_class)
        self._session_active = True
        self._session_start_time = datetime.now(timezone.utc)
        self._last_completed_laps = snapshot.completed_laps
        
        # Capture initial setup state
        self._current_setup = self._extract_setup(snapshot)
        
        event = SessionEvent(
            event_type="session_start",
            session_id=self._current_session_id,
            platform="ACC",
            track=snapshot.track,
            car=snapshot.car,
            session_type=snapshot.session_type,
            timestamp=datetime.now(timezone.utc).isoformat(),
        )
        
        logger.info(f"🏁 Session started: {self._current_session_id} | {snapshot.track} | {snapshot.car}")
        self._emit(self._session_start_callbacks, event)
        
        # Also emit initial setup snapshot
        setup_snap = SetupSnapshot(
            session_id=self._current_session_id,
            car=snapshot.car,
            track=snapshot.track,
            timestamp=datetime.now(timezone.utc).isoformat(),
            lap_applied_from=0,
            **self._current_setup,
        )
        self._emit(self._setup_snapshot_callbacks, setup_snap)
    
    def _end_session(self, snapshot: ACCTelemetrySnapshot):
        """Handle session end."""
        prev = self._previous_snapshot
        
        event = SessionEvent(
            event_type="session_end",
            session_id=self._current_session_id or "unknown",
            platform="ACC",
            track=prev.track if prev else "",
            car=prev.car if prev else "",
            session_type=prev.session_type if prev else "",
            timestamp=datetime.now(timezone.utc).isoformat(),
            total_laps=prev.completed_laps if prev else None,
            best_lap_ms=prev.best_lap_time_ms if prev else None,
        )
        
        logger.info(f"🏁 Session ended: {self._current_session_id} | {event.total_laps} laps")
        self._emit(self._session_end_callbacks, event)
        
        self._session_active = False
        self._current_session_id = None
        self._current_setup = {}
    
    def _handle_lap_completion(self, snapshot: ACCTelemetrySnapshot):
        """Handle a detected lap boundary crossing."""
        event = LapCompletionEvent(
            session_id=self._current_session_id or "unknown",
            lap_number=snapshot.completed_laps,  # This is the lap that just completed
            lap_time_ms=snapshot.last_lap_time_ms,
            last_sector_time_ms=snapshot.last_sector_time_ms,
            is_valid=snapshot.is_valid_lap,
            fuel_remaining=snapshot.fuel_remaining,
            timestamp=datetime.now(timezone.utc).isoformat(),
            
            tyre_temp_fl=snapshot.wheel_fl.core_temp,
            tyre_temp_fr=snapshot.wheel_fr.core_temp,
            tyre_temp_rl=snapshot.wheel_rl.core_temp,
            tyre_temp_rr=snapshot.wheel_rr.core_temp,
            tyre_pressure_fl=snapshot.wheel_fl.pressure,
            tyre_pressure_fr=snapshot.wheel_fr.pressure,
            tyre_pressure_rl=snapshot.wheel_rl.pressure,
            tyre_pressure_rr=snapshot.wheel_rr.pressure,
        )
        
        valid_str = "✅" if event.is_valid else "❌"
        time_str = f"{event.lap_time_ms / 1000:.3f}s"
        logger.info(f"  Lap {event.lap_number}: {time_str} {valid_str}")
        self._emit(self._lap_completed_callbacks, event)
    
    def _extract_setup(self, snapshot: ACCTelemetrySnapshot) -> dict:
        """Extract tracked setup parameters from a snapshot."""
        return {
            param: getattr(snapshot, param)
            for param in self.TRACKED_SETUP_PARAMS
        }
    
    def _check_setup_changes(self, snapshot: ACCTelemetrySnapshot):
        """Detect and emit setup parameter changes."""
        new_setup = self._extract_setup(snapshot)
        
        for param in self.TRACKED_SETUP_PARAMS:
            old_val = self._current_setup.get(param)
            new_val = new_setup.get(param)
            
            if old_val is not None and old_val != new_val:
                # For floats, use a tolerance to avoid noise
                if isinstance(old_val, float) and abs(old_val - new_val) < 0.01:
                    continue
                
                event = SetupChangeEvent(
                    session_id=self._current_session_id or "unknown",
                    parameter=param,
                    old_value=old_val,
                    new_value=new_val,
                    lap_number=snapshot.completed_laps,
                    timestamp=datetime.now(timezone.utc).isoformat(),
                )
                
                logger.info(f"  🔧 Setup change: {param} {old_val} → {new_val}")
                self._emit(self._setup_change_callbacks, event)
                
                # Also emit a full setup snapshot
                setup_snap = SetupSnapshot(
                    session_id=self._current_session_id or "unknown",
                    car=snapshot.car,
                    track=snapshot.track,
                    timestamp=datetime.now(timezone.utc).isoformat(),
                    lap_applied_from=snapshot.completed_laps,
                    **new_setup,
                )
                self._emit(self._setup_snapshot_callbacks, setup_snap)
        
        self._current_setup = new_setup
    
    @property
    def current_session_id(self) -> Optional[str]:
        return self._current_session_id
    
    @property
    def is_session_active(self) -> bool:
        return self._session_active
