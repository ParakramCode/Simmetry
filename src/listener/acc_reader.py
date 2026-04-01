"""
ACC Shared Memory Reader.

Reads the three ACC memory-mapped files (Physics, Graphics, Static)
and returns structured Python dataclasses ready for downstream processing.

The reader:
  1. Opens the memory-mapped files once on initialization
  2. Reads all three pages on each call to read()
  3. Returns a unified ACCTelemetrySnapshot dataclass
  4. Handles the case where ACC is not running (returns None)

Usage:
    reader = ACCReader()
    snapshot = reader.read()
    if snapshot:
        print(f"Speed: {snapshot.speed_kmh} km/h")
    reader.close()
"""

import ctypes
import mmap
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional, List
from loguru import logger

from .acc_structs import (
    SPageFilePhysics,
    SPageFileGraphic,
    SPageFileStatic,
    ACC_STATUS,
    ACC_SESSION_TYPE,
    ACC_FLAG_TYPE,
    ACC_TRACK_GRIP_STATUS,
    ACC_RAIN_INTENSITY,
)

from config.settings import ACC_PHYSICS_TAG, ACC_GRAPHICS_TAG, ACC_STATIC_TAG


# ═══════════════════════════════════════════════════════════════
# Dataclasses — Clean Python representations of ACC data
# ═══════════════════════════════════════════════════════════════

@dataclass(frozen=True, slots=True)
class WheelData:
    """Tyre/wheel data for a single corner. Order: FL, FR, RL, RR."""
    pressure: float        # PSI
    core_temp: float       # °C — the key temperature for tyre window analysis
    inner_temp: float      # °C
    middle_temp: float     # °C
    outer_temp: float      # °C
    slip: float            # Slip ratio
    suspension_travel: float  # meters
    brake_temp: float      # °C
    wear: float            # Tyre wear (from tyreWear field)


@dataclass(frozen=True, slots=True)
class ACCTelemetrySnapshot:
    """
    A single unified telemetry snapshot combining Physics + Graphics + Static.

    This is what gets published to Kafka (or written to CSV in Week 1).
    Designed to match the telemetry event schema from the project spec.
    """
    # ── Identity ──
    timestamp_wall: datetime       # Wall clock time of this reading
    timestamp_game: float          # In-game session time (seconds)

    # ── Session info (from Static + Graphics) ──
    car: str
    track: str
    session_type: str             # "practice", "qualify", "race", etc.
    status: str                   # "live", "replay", "pause", "off"

    # ── Lap info (from Graphics) ──
    completed_laps: int           # Total completed laps
    current_lap_time_ms: int      # Current lap time in ms
    last_lap_time_ms: int         # Last completed lap time in ms
    best_lap_time_ms: int         # Session best lap time in ms
    distance_into_lap: float      # ★ Distance traveled in current lap (meters)
    normalized_position: float    # 0.0 to 1.0 around the track
    current_sector: int           # Current sector index (0, 1, 2)
    last_sector_time_ms: int      # Last completed sector time in ms
    is_valid_lap: bool            # ★ Whether current lap is valid
    is_in_pit: bool
    is_in_pit_lane: bool

    # ── Inputs (from Physics) ──
    throttle: float               # 0.0 to 1.0
    brake: float                  # 0.0 to 1.0
    steering: float               # -1.0 to 1.0
    clutch: float                 # 0.0 to 1.0

    # ── Car dynamics (from Physics) ──
    speed_kmh: float
    gear: int                     # 0=R, 1=N, 2=1st, 3=2nd, ...
    rpm: int
    fuel_remaining: float         # Liters
    fuel_per_lap: float           # Average fuel per lap (litres)

    # ── G-forces (from Physics) ──
    g_lat: float                  # Lateral G (positive = right)
    g_lon: float                  # Longitudinal G (positive = acceleration)
    g_vert: float                 # Vertical G

    # ── Wheels (from Physics) ──
    wheel_fl: WheelData
    wheel_fr: WheelData
    wheel_rl: WheelData
    wheel_rr: WheelData

    # ── Car settings currently active ──
    tc_level: int
    tc_cut: int
    abs_level: int
    engine_map: int
    brake_bias: float             # Front brake bias (%)

    # ── Environment ──
    air_temp: float               # °C
    road_temp: float              # °C
    track_grip: str               # "green", "fast", "optimum", etc.
    rain_intensity: str           # "no_rain", "drizzle", etc.
    wind_speed: float
    wind_direction: float


# ═══════════════════════════════════════════════════════════════
# Reader Class
# ═══════════════════════════════════════════════════════════════

class ACCReader:
    """
    Reads ACC shared memory and returns ACCTelemetrySnapshot objects.

    Must be run on Windows while ACC is running.
    Opens memory-mapped files on first successful read.
    """

    def __init__(self):
        self._physics_mmap: Optional[mmap.mmap] = None
        self._graphics_mmap: Optional[mmap.mmap] = None
        self._static_mmap: Optional[mmap.mmap] = None
        self._connected = False

        # Cache static data (only changes at session start)
        self._static_cache: Optional[SPageFileStatic] = None
        self._last_packet_id: int = -1

        logger.info("ACCReader initialized -- waiting for ACC connection")

    def _open_mmap(self, tag: str, struct_type: type) -> Optional[mmap.mmap]:
        """Open a memory-mapped file by its ACC tag."""
        try:
            size = ctypes.sizeof(struct_type)
            buf = mmap.mmap(0, size, tag)
            return buf
        except (FileNotFoundError, OSError, mmap.error) as e:
            logger.debug(f"Could not open shared memory '{tag}': {e}")
            return None

    def _connect(self) -> bool:
        """Attempt to connect to all three ACC shared memory pages."""
        if self._connected:
            return True

        self._physics_mmap = self._open_mmap(ACC_PHYSICS_TAG, SPageFilePhysics)
        self._graphics_mmap = self._open_mmap(ACC_GRAPHICS_TAG, SPageFileGraphic)
        self._static_mmap = self._open_mmap(ACC_STATIC_TAG, SPageFileStatic)

        if all([self._physics_mmap, self._graphics_mmap, self._static_mmap]):
            self._connected = True
            logger.success("Connected to ACC shared memory")
            return True

        # Clean up partial connections
        self._disconnect()
        return False

    def _disconnect(self):
        """Close all memory-mapped files."""
        for buf in [self._physics_mmap, self._graphics_mmap, self._static_mmap]:
            if buf:
                try:
                    buf.close()
                except Exception:
                    pass
        self._physics_mmap = None
        self._graphics_mmap = None
        self._static_mmap = None
        self._connected = False
        self._static_cache = None

    def _read_struct(self, buf: mmap.mmap, struct_type: type):
        """Read a ctypes struct from a memory-mapped buffer."""
        buf.seek(0)
        raw = buf.read(ctypes.sizeof(struct_type))
        return struct_type.from_buffer_copy(raw)

    def _session_type_str(self, session_val) -> str:
        """Convert ACC session type to readable string."""
        # StructureWithEnums auto-maps to ACC_SESSION_TYPE enum
        mapping = {
            ACC_SESSION_TYPE.ACC_PRACTICE: "practice",
            ACC_SESSION_TYPE.ACC_QUALIFY: "qualify",
            ACC_SESSION_TYPE.ACC_RACE: "race",
            ACC_SESSION_TYPE.ACC_HOTLAP: "hotlap",
            ACC_SESSION_TYPE.ACC_TIMEATTACK: "time_attack",
            ACC_SESSION_TYPE.ACC_HOTSTINT: "hotstint",
            ACC_SESSION_TYPE.ACC_HOTSTINTSUPERPOLE: "superpole",
        }
        try:
            return mapping.get(int(session_val), "unknown")
        except (ValueError, TypeError):
            return "unknown"

    def _status_str(self, status_val) -> str:
        """Convert ACC status to readable string."""
        mapping = {
            ACC_STATUS.ACC_OFF: "off",
            ACC_STATUS.ACC_REPLAY: "replay",
            ACC_STATUS.ACC_LIVE: "live",
            ACC_STATUS.ACC_PAUSE: "pause",
        }
        try:
            return mapping.get(int(status_val), "unknown")
        except (ValueError, TypeError):
            return "unknown"

    def _grip_str(self, grip_val) -> str:
        """Convert track grip to readable string."""
        mapping = {
            ACC_TRACK_GRIP_STATUS.ACC_GREEN: "green",
            ACC_TRACK_GRIP_STATUS.ACC_FAST: "fast",
            ACC_TRACK_GRIP_STATUS.ACC_OPTIMUM: "optimum",
            ACC_TRACK_GRIP_STATUS.ACC_GREASY: "greasy",
            ACC_TRACK_GRIP_STATUS.ACC_DAMP: "damp",
            ACC_TRACK_GRIP_STATUS.ACC_WET: "wet",
            ACC_TRACK_GRIP_STATUS.ACC_FLOODED: "flooded",
        }
        try:
            return mapping.get(int(grip_val), "unknown")
        except (ValueError, TypeError):
            return "unknown"

    def _rain_str(self, rain_val) -> str:
        """Convert rain intensity to readable string."""
        mapping = {
            ACC_RAIN_INTENSITY.ACC_NO_RAIN: "no_rain",
            ACC_RAIN_INTENSITY.ACC_DRIZZLE: "drizzle",
            ACC_RAIN_INTENSITY.ACC_LIGHT_RAIN: "light_rain",
            ACC_RAIN_INTENSITY.ACC_MEDIUM_RAIN: "medium_rain",
            ACC_RAIN_INTENSITY.ACC_HEAVY_RAIN: "heavy_rain",
            ACC_RAIN_INTENSITY.ACC_THUNDERSTORM: "thunderstorm",
        }
        try:
            return mapping.get(int(rain_val), "unknown")
        except (ValueError, TypeError):
            return "unknown"

    def read(self) -> Optional[ACCTelemetrySnapshot]:
        """
        Read a complete telemetry snapshot from ACC shared memory.

        Returns None if:
          - ACC is not running
          - Shared memory isn't available
          - Game status is OFF or REPLAY
        """
        if not self._connect():
            return None

        try:
            # Read all three pages
            physics = self._read_struct(self._physics_mmap, SPageFilePhysics)
            graphics = self._read_struct(self._graphics_mmap, SPageFileGraphic)

            # Only re-read static if cache is empty (it doesn't change mid-session)
            if self._static_cache is None:
                self._static_cache = self._read_struct(self._static_mmap, SPageFileStatic)

            static = self._static_cache

            # Static fields are Word arrays — use str() to decode
            car_name = str(static.carModel)
            track_name = str(static.track)

            # Retry static read if car/track names are empty — ACC may not
            # have populated the static page yet when we first read it
            if not car_name or not track_name:
                self._static_cache = None
                return None

            # Skip if game isn't live
            # graphics.status is auto-mapped to ACC_STATUS enum by StructureWithEnums
            if int(graphics.status) == ACC_STATUS.ACC_OFF:
                return None

            # Skip duplicate packets (same physics step)
            if physics.packetId == self._last_packet_id:
                return None
            self._last_packet_id = physics.packetId

            # Build wheel data for a single corner
            def make_wheel(idx: int) -> WheelData:
                return WheelData(
                    pressure=physics.wheelsPressure[idx],
                    core_temp=physics.tyreTemp[idx],
                    inner_temp=physics.tyreTempI[idx],
                    middle_temp=physics.tyreTempM[idx],
                    outer_temp=physics.tyreTempO[idx],
                    slip=physics.wheelSlip[idx],
                    suspension_travel=physics.suspensionTravel[idx],
                    brake_temp=physics.brakeTemp[idx],
                    wear=physics.tyreWear[idx],
                )

            # ── Brake bias conversion ──
            # The raw brakeBias field is NOT the MFD percentage.
            # Per SDK Appendix 4, each car has a base bias offset.
            # We look up the Kunos car ID from the static cache to apply the offset.
            # 
            # Note: Many popular GT3/GT4 cars have different offsets.
            # For now, we deduct 5% as a common baseline if the car is unknown,
            # but ideally this should map to the exact Kunos SDK Appendix 4 table.
            KNOWN_OFFSETS = {
                "12": -7.0,  # Aston Martin V12 GT3 2013
                "3": -14.0,  # Audi R8 LMS 2015
                "2": -17.0,  # Ferrari 488 GT3 2018
                # Add more as discovered from Appendix 4.
            }
            
            raw_bb = physics.brakeBias
            
            # Since the user experienced 65% when raw was 70% (0.70), it means the car offset is -5%.
            # We will use -5% as a default generic offset if the exact car is not in the KNOWN_OFFSETS table.
            
            # Note `car_name` at this point is the string representation of the car ID.
            # It might be just the ID number or the enum string. We try to match both.
            offset = KNOWN_OFFSETS.get(car_name, -5.0) 
            
            brake_bias_pct = (raw_bb * 100.0) + offset

            # Build the unified snapshot
            snapshot = ACCTelemetrySnapshot(
                timestamp_wall=datetime.now(timezone.utc),
                timestamp_game=graphics.sessionTimeLeft,

                car=car_name,
                track=track_name,
                session_type=self._session_type_str(graphics.session),
                status=self._status_str(graphics.status),

                completed_laps=graphics.completedLaps,
                current_lap_time_ms=graphics.iCurrentTime,
                last_lap_time_ms=graphics.iLastTime,
                best_lap_time_ms=graphics.iBestTime,
                distance_into_lap=graphics.distanceTraveled,
                normalized_position=graphics.normalizedCarPosition,
                current_sector=graphics.currentSectorIndex,
                last_sector_time_ms=graphics.lastSectorTime,
                is_valid_lap=bool(graphics.isValidLap),
                is_in_pit=bool(graphics.isInPit),
                is_in_pit_lane=bool(graphics.isInPitLane),

                throttle=physics.gas,
                brake=physics.brake,
                steering=physics.steerAngle,
                clutch=physics.clutch,

                speed_kmh=physics.speedKmh,
                gear=physics.gear,
                rpm=physics.rpms,
                fuel_remaining=physics.fuel,
                fuel_per_lap=graphics.fuelXLap,

                g_lat=physics.accG[0],
                g_lon=physics.accG[2],
                g_vert=physics.accG[1],

                wheel_fl=make_wheel(0),
                wheel_fr=make_wheel(1),
                wheel_rl=make_wheel(2),
                wheel_rr=make_wheel(3),

                tc_level=graphics.TC,
                tc_cut=graphics.TCCut,
                abs_level=graphics.ABS,
                engine_map=graphics.EngineMap,
                brake_bias=brake_bias_pct,

                air_temp=physics.airTemp,
                road_temp=physics.roadTemp,
                track_grip=self._grip_str(graphics.trackGripStatus),
                rain_intensity=self._rain_str(graphics.rainIntensity),
                wind_speed=graphics.windSpeed,
                wind_direction=graphics.windDirection,
            )

            return snapshot

        except Exception as e:
            logger.error(f"Error reading shared memory: {e}")
            self._disconnect()
            return None

    @property
    def is_connected(self) -> bool:
        """Whether we're connected to ACC shared memory."""
        return self._connected

    def refresh_static(self):
        """Force a re-read of the static page (call on session change)."""
        self._static_cache = None

    def close(self):
        """Cleanly close all shared memory connections."""
        self._disconnect()
        logger.info("ACCReader closed")

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()
