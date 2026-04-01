"""
ACC Shared Memory Structure Definitions.

Verified against the reference implementation from PyAccSharedMemory.
Uses LittleEndianStructure (explicit byte order) and custom Word types
for correct string field handling.

ACC exposes three memory-mapped files:
  - "Local\\acpmf_physics"  — updated at ~333Hz (car dynamics)
  - "Local\\acpmf_graphics" — updated at ~60Hz  (session/timing)
  - "Local\\acpmf_static"   — updated once      (car/track info)

CRITICAL: Field order and types must EXACTLY match the C++ structs.
A single missing or mis-typed field shifts all subsequent offsets,
causing every field after it to read garbage.

References:
  - PyAccSharedMemory by rrennoir (MIT): github.com/rrennoir/PyAccSharedMemory
  - ACC Shared Memory Documentation v1.8.12 (Kunos Simulazioni)
"""

from ctypes import LittleEndianStructure, c_int, c_float, Array, wintypes
from enum import IntEnum


# ═══════════════════════════════════════════════════════════════
# Custom String Types — Match ACC's wide-char string layout
# ═══════════════════════════════════════════════════════════════
#
# ACC uses wchar_t arrays (UTF-16 on Windows).
# wintypes.WORD = c_ushort = 2 bytes, matching Windows wchar_t.
# These custom Array types add proper string decoding.

class ShortWord(Array):
    """15-element WORD array — used for time strings ("mm:ss.SSS")."""
    _type_ = wintypes.WORD
    _length_ = 15

    def __str__(self):
        try:
            return bytes(self).decode('utf-16-le').rstrip('\x00')
        except UnicodeDecodeError:
            return ""


class Word(Array):
    """33-element WORD array — used for names (car, track, player)."""
    _type_ = wintypes.WORD
    _length_ = 33

    def __str__(self):
        try:
            return bytes(self).decode('utf-16-le').rstrip('\x00')
        except UnicodeDecodeError:
            return ""


# ═══════════════════════════════════════════════════════════════
# StructureWithEnums — Auto-map int fields to IntEnum values
# ═══════════════════════════════════════════════════════════════
# Credit: https://gist.github.com/christoph2/9c390e5c094796903097

class StructureWithEnums(LittleEndianStructure):
    """Add missing enum feature to ctypes Structures."""
    _map = {}

    def __getattribute__(self, name):
        _map = LittleEndianStructure.__getattribute__(self, '_map')
        value = LittleEndianStructure.__getattribute__(self, name)
        if name in _map:
            EnumClass = _map[name]
            if isinstance(value, Array):
                return [EnumClass(x) for x in value]
            else:
                return EnumClass(value)
        else:
            return value

    def __str__(self):
        result = []
        result.append("struct {0} {{".format(self.__class__.__name__))
        for field in self._fields_:
            attr, attrType = field[:2]
            if attr in self._map:
                attrType = self._map[attr]
            value = getattr(self, attr)
            result.append("    {0} [{1}] = {2!r};".format(attr, attrType.__name__, value))
        result.append("};")
        return '\n'.join(result)

    __repr__ = __str__


# ═══════════════════════════════════════════════════════════════
# Enums — Match ACC's internal integer values
# ═══════════════════════════════════════════════════════════════

class ACC_STATUS(IntEnum):
    ACC_OFF = 0
    ACC_REPLAY = 1
    ACC_LIVE = 2
    ACC_PAUSE = 3


class ACC_SESSION_TYPE(IntEnum):
    ACC_UNKNOWN = -1
    ACC_PRACTICE = 0
    ACC_QUALIFY = 1
    ACC_RACE = 2
    ACC_HOTLAP = 3
    ACC_TIMEATTACK = 4
    ACC_DRIFT = 5
    ACC_DRAG = 6
    ACC_HOTSTINT = 7
    ACC_HOTSTINTSUPERPOLE = 8


class ACC_FLAG_TYPE(IntEnum):
    ACC_NO_FLAG = 0
    ACC_BLUE_FLAG = 1
    ACC_YELLOW_FLAG = 2
    ACC_BLACK_FLAG = 3
    ACC_WHITE_FLAG = 4
    ACC_CHECKERED_FLAG = 5
    ACC_PENALTY_FLAG = 6
    ACC_GREEN_FLAG = 7
    ACC_ORANGE_FLAG = 8


class ACC_PENALTY_TYPE(IntEnum):
    ACC_None = 0
    ACC_DriveThrough_Cutting = 1
    ACC_StopAndGo_10_Cutting = 2
    ACC_StopAndGo_20_Cutting = 3
    ACC_StopAndGo_30_Cutting = 4
    ACC_Disqualified_Cutting = 5
    ACC_RemoveBestLaptime_Cutting = 6
    ACC_DriveThrough_PitSpeeding = 7
    ACC_StopAndGo_10_PitSpeeding = 8
    ACC_StopAndGo_20_PitSpeeding = 9
    ACC_StopAndGo_30_PitSpeeding = 10
    ACC_Disqualified_PitSpeeding = 11
    ACC_RemoveBestLaptime_PitSpeeding = 12
    ACC_Disqualified_IgnoredMandatoryPit = 13
    ACC_PostRaceTime = 14
    ACC_Disqualified_Trolling = 15
    ACC_Disqualified_PitEntry = 16
    ACC_Disqualified_PitExit = 17
    ACC_Disqualified_Wrongway = 18
    ACC_DriveThrough_IgnoredDriverStint = 19
    ACC_Disqualified_IgnoredDriverStint = 20
    ACC_Disqualified_ExceededDriverStintLimit = 21


class ACC_WHEELS_TYPE(IntEnum):
    ACC_FrontLeft = 0
    ACC_FrontRight = 1
    ACC_RearLeft = 2
    ACC_RearRight = 3


class ACC_TRACK_GRIP_STATUS(IntEnum):
    ACC_GREEN = 0
    ACC_FAST = 1
    ACC_OPTIMUM = 2
    ACC_GREASY = 3
    ACC_DAMP = 4
    ACC_WET = 5
    ACC_FLOODED = 6


class ACC_RAIN_INTENSITY(IntEnum):
    ACC_NO_RAIN = 0
    ACC_DRIZZLE = 1
    ACC_LIGHT_RAIN = 2
    ACC_MEDIUM_RAIN = 3
    ACC_HEAVY_RAIN = 4
    ACC_THUNDERSTORM = 5


# ═══════════════════════════════════════════════════════════════
# Physics Page — High-frequency car dynamics (~333Hz)
# ═══════════════════════════════════════════════════════════════

class SPageFilePhysics(LittleEndianStructure):
    """
    ACC Physics shared memory page.

    Contains real-time car dynamics: speed, inputs, tyres, suspension,
    G-forces, engine data. Updated at ~60Hz.

    Memory tag: "Local\\acpmf_physics"
    """
    _fields_ = [
        ('packetId', c_int),
        ('gas', c_float),                           # Throttle input [0.0 - 1.0]
        ('brake', c_float),                         # Brake input [0.0 - 1.0]
        ('fuel', c_float),                          # Fuel remaining (liters)
        ('gear', c_int),                            # Current gear (0=R, 1=N, 2=1st, etc.)
        ('rpms', c_int),                            # Engine RPM
        ('steerAngle', c_float),                    # Steering angle [-1.0 to 1.0]
        ('speedKmh', c_float),                      # Car speed (km/h)

        # Velocity vector (m/s) — world coordinates
        ('velocity', c_float * 3),                  # [x, y, z]

        # Acceleration vector (G)
        ('accG', c_float * 3),                      # [x(lat), y(up), z(lon)]

        # Wheel data — order: [FL, FR, RL, RR]
        ('wheelSlip', c_float * 4),                 # Tyre slip ratio
        ('wheelLoad', c_float * 4),                 # Vertical load (N) [DEPRECATED]
        ('wheelsPressure', c_float * 4),            # Tyre pressure (PSI)
        ('wheelAngularSpeed', c_float * 4),         # Wheel rotation speed (rad/s)
        ('tyreWear', c_float * 4),                  # ★ Tyre wear [0.0 = new, higher = worn]
        ('tyreDirtyLevel', c_float * 4),            # Tyre dirty level
        ('tyreCoreTemperature', c_float * 4),       # Tyre core temp (°C)
        ('camberRAD', c_float * 4),                 # ★ Camber angle (radians)
        ('suspensionTravel', c_float * 4),          # Suspension travel (meters)

        ('drs', c_float),                           # ★ DRS value
        ('tc', c_float),                            # TC slip ratio
        ('heading', c_float),                       # Car heading (rad)
        ('pitch', c_float),                         # Car pitch (rad)
        ('roll', c_float),                          # Car roll (rad)
        ('cgHeight', c_float),                      # Centre of gravity height

        # Car damage
        ('carDamage', c_float * 5),                 # [front, rear, left, right, centre]

        ('numberOfTyresOut', c_int),                # ★ Number of tyres off track
        ('pitLimiterOn', c_int),                    # Pit limiter active

        # ABS
        ('abs', c_float),                           # ABS slip ratio

        # KERS / hybrid
        ('kersCharge', c_float),
        ('kersInput', c_float),
        ('autoShifterOn', c_int),
        ('rideHeight', c_float * 2),                # [front, rear] in mm

        # Turbo / environment
        ('turboBoost', c_float),
        ('ballast', c_float),
        ('airDensity', c_float),
        ('airTemp', c_float),                       # Air temperature (°C)
        ('roadTemp', c_float),                      # Road surface temperature (°C)

        # Local angular velocity
        ('localAngularVel', c_float * 3),

        ('finalFF', c_float),                       # Force feedback value
        ('performanceMeter', c_float),
        ('engineBrake', c_int),
        ('ersRecoveryLevel', c_int),
        ('ersPowerLevel', c_int),
        ('ersHeatCharging', c_int),
        ('ersIsCharging', c_int),
        ('kersCurrentKJ', c_float),                 # Not used in ACC
        ('drsAvailable', c_int),
        ('drsEnabled', c_int),

        # Brake temperatures
        ('brakeTemp', c_float * 4),                 # Brake disc temp (°C) [FL, FR, RL, RR]

        # Clutch
        ('clutch', c_float),

        # Tyre temperature zones
        ('tyreTempI', c_float * 4),                 # Inner tyre temp
        ('tyreTempM', c_float * 4),                 # Middle tyre temp
        ('tyreTempO', c_float * 4),                 # Outer tyre temp
        ('isAIControlled', c_int),

        # Tyre contact point (world position) — 4 wheels × [x, y, z]
        ('tyreContactPoint', c_float * 3 * 4),
        ('tyreContactNormal', c_float * 3 * 4),
        ('tyreContactHeading', c_float * 3 * 4),

        ('brakeBias', c_float),                     # Front brake bias (%)

        ('localVelocity', c_float * 3),             # Car-local velocity [x, y, z]

        # Push-to-pass
        ('P2PActivations', c_int),
        ('P2PStatus', c_int),
        ('currentMaxRpm', c_float),                 # Maximum engine RPM

        # Tyre forces
        ('mz', c_float * 4),                        # Tyre self-aligning torque
        ('fx', c_float * 4),                        # Tyre longitudinal force
        ('fy', c_float * 4),                        # Tyre lateral force
        ('slipRatio', c_float * 4),
        ('slipAngle', c_float * 4),

        ('tcInAction', c_int),
        ('absInAction', c_int),
        ('suspensionDamage', c_float * 4),
        ('tyreTemp', c_float * 4),                  # ★ Tyre core temps — use THIS, not tyreCoreTemperature

        # Water temp / brake pressure
        ('waterTemp', c_float),
        ('brakePressure', c_float * 4),
        ('frontBrakeCompound', c_int),
        ('rearBrakeCompound', c_int),
        ('padLife', c_float * 4),
        ('discLife', c_float * 4),

        # Ignition / starter / engine state
        ('ignitionOn', c_int),
        ('starterEngineOn', c_int),
        ('isEngineRunning', c_int),              # ★ Engine running state

        # Vibration feedback
        ('kerbVibration', c_float),
        ('slipVibrations', c_float),
        ('gVibrations', c_float),
        ('absVibrations', c_float),
    ]


# ═══════════════════════════════════════════════════════════════
# Graphics Page — Session/timing data (~30Hz)
# ═══════════════════════════════════════════════════════════════

class SPageFileGraphic(StructureWithEnums):
    """
    ACC Graphics shared memory page.

    Contains session status, lap counts, sector times, flags, weather,
    and car position data. Updated at ~60Hz.

    Memory tag: "Local\\acpmf_graphics"

    The lap timing fields here are THE authoritative source for lap times.
    """
    _fields_ = [
        ("packetId", c_int),
        ("status", c_int),                          # ACC_STATUS enum
        ("session", c_int),                         # ACC_SESSION_TYPE enum

        # Current lap time (string, wide chars)
        ("currentTime", ShortWord),                 # "mm:ss.SSS"
        ("lastTime", ShortWord),
        ("bestTime", ShortWord),
        ("split", ShortWord),

        ("completedLaps", c_int),                   # Total completed laps
        ("position", c_int),                        # Race position

        # Timing (milliseconds) — authoritative integer values
        ("iCurrentTime", c_int),                    # Current lap time in ms
        ("iLastTime", c_int),                       # Last lap time in ms
        ("iBestTime", c_int),                       # Best lap time in ms

        ("sessionTimeLeft", c_float),               # Remaining session time (ms)
        ("distanceTraveled", c_float),              # ★ Distance into current lap (m)

        ("isInPit", c_int),
        ("currentSectorIndex", c_int),              # Current sector (0, 1, 2)
        ("lastSectorTime", c_int),                  # Last completed sector time (ms)

        ("numberOfLaps", c_int),
        ("tyreCompound", Word),

        # Replay / position data
        ("replayTimeMultiplier", c_float),
        ("normalizedCarPosition", c_float),         # 0.0 to 1.0 around the track

        ("activeCars", c_int),
        ("carCoordinates", c_float * 3 * 60),       # Up to 60 cars × [x, y, z]
        ("carID", c_int * 60),
        ("playerCarID", c_int),

        ("penaltyTime", c_float),
        ("flag", c_int),                            # ACC_FLAG_TYPE enum
        ("penalty", c_int),                         # ACC_PENALTY_TYPE enum
        ("idealLineOn", c_int),
        ("isInPitLane", c_int),

        ("surfaceGrip", c_float),
        ("mandatoryPitDone", c_int),
        ("windSpeed", c_float),
        ("windDirection", c_float),

        ("isSetupMenuVisible", c_int),
        ("mainDisplayIndex", c_int),
        ("secondaryDisplayIndex", c_int),
        ("TC", c_int),                              # TC level setting
        ("TCCut", c_int),                           # TC Cut level
        ("EngineMap", c_int),
        ("ABS", c_int),                             # ABS level setting
        ("fuelXLap", c_float),                      # Average fuel per lap (litres)
        ("rainLights", c_int),
        ("flashingLights", c_int),
        ("lightsStage", c_int),
        ("exhaustTemperature", c_float),
        ("wiperLV", c_int),

        ("DriverStintTotalTimeLeft", c_int),
        ("DriverStintTimeLeft", c_int),
        ("rainTyres", c_int),                       # Using wet tyres

        ("sessionIndex", c_int),
        ("usedFuel", c_float),                      # Total fuel used in session

        # Delta bar
        ("deltaLapTime", ShortWord),
        ("iDeltaLapTime", c_int),                   # Delta to best lap (ms)
        ("estimatedLapTime", ShortWord),
        ("iEstimatedLapTime", c_int),               # Estimated lap time (ms)

        ("isDeltaPositive", c_int),
        ("iSplit", c_int),                          # Last split time (ms)
        ("isValidLap", c_int),                      # ★ Current lap validity

        ("fuelEstimatedLaps", c_float),
        ("trackStatus", Word),

        ("missingMandatoryPits", c_int),
        ("Clock", c_float),                         # In-game clock (seconds from midnight)

        ("directionLightsLeft", c_int),
        ("directionLightsRight", c_int),
        ("GlobalYellow", c_int),
        ("GlobalYellow1", c_int),
        ("GlobalYellow2", c_int),
        ("GlobalYellow3", c_int),
        ("GlobalWhite", c_int),
        ("GlobalGreen", c_int),
        ("GlobalChequered", c_int),
        ("GlobalRed", c_int),
        ("mfdTyreSet", c_int),
        ("mfdFuelToAdd", c_float),
        ("mfdTyrePressureLF", c_float),
        ("mfdTyrePressureRF", c_float),
        ("mfdTyrePressureLR", c_float),
        ("mfdTyrePressureRR", c_float),

        ("trackGripStatus", c_int),                 # ACC_TRACK_GRIP_STATUS enum
        ("rainIntensity", c_int),                   # ACC_RAIN_INTENSITY enum
        ("rainIntensityIn10min", c_int),
        ("rainIntensityIn30min", c_int),
        ("currentTyreSet", c_int),
        ("strategyTyreSet", c_int),
        ("gapAhead", c_int),                        # Gap to car ahead (ms)
        ("gapBehind", c_int),                       # Gap to car behind (ms)
    ]
    _map = {
        "status": ACC_STATUS,
        "session": ACC_SESSION_TYPE,
        "flag": ACC_FLAG_TYPE,
        "penalty": ACC_PENALTY_TYPE,
        "trackGripStatus": ACC_TRACK_GRIP_STATUS,
        "rainIntensity": ACC_RAIN_INTENSITY,
        "rainIntensityIn10min": ACC_RAIN_INTENSITY,
        "rainIntensityIn30min": ACC_RAIN_INTENSITY,
    }


# ═══════════════════════════════════════════════════════════════
# Static Page — Session constants (set once)
# ═══════════════════════════════════════════════════════════════

class SPageFileStatic(LittleEndianStructure):
    """
    ACC Static shared memory page.

    Contains session constants: car model, track name, player info.
    Only updated at session initialization.

    Memory tag: "Local\\acpmf_static"
    """
    _fields_ = [
        ("smVersion", ShortWord),
        ("acVersion", ShortWord),

        ("numberOfSessions", c_int),
        ("numCars", c_int),
        ("carModel", Word),                         # ★ Car name
        ("track", Word),                            # ★ Track name
        ("playerName", Word),
        ("playerSurname", Word),
        ("playerNick", Word),

        ("sectorCount", c_int),                     # Number of sectors

        # Track-specific constants
        ("maxTorque", c_float),
        ("maxPower", c_float),
        ("maxRpm", c_int),
        ("maxFuel", c_float),
        ("suspensionMaxTravel", c_float * 4),
        ("tyreRadius", c_float * 4),

        ("maxTurboBoost", c_float),

        ("deprecated_1", c_float),
        ("deprecated_2", c_float),

        ("penaltiesEnabled", c_int),
        ("aidFuelRate", c_float),
        ("aidTireRate", c_float),
        ("aidMechanicalDamage", c_float),
        ("aidAllowTyreBlankets", c_float),
        ("aidStability", c_float),
        ("aidAutoClutch", c_int),
        ("aidAutoBlip", c_int),

        ("hasDRS", c_int),
        ("hasERS", c_int),
        ("hasKERS", c_int),
        ("kersMaxJ", c_float),
        ("engineBrakeSettingsCount", c_int),
        ("ersPowerControllerCount", c_int),
        ("trackSPlineLength", c_float),             # ★ Total track length in meters
        ("trackConfiguration", Word),
        ("ersMaxJ", c_float),

        ("isTimedRace", c_int),
        ("hasExtraLap", c_int),
        ("carSkin", Word),
        ("reversedGridPositions", c_int),
        ("PitWindowStart", c_int),
        ("PitWindowEnd", c_int),
        ("isOnline", c_int),
        ("dryTyresName", Word),
        ("wetTyresName", Word),
    ]


# ═══════════════════════════════════════════════════════════════
# Car Model Data — KunosID → Name, maxRPM, maxSteeringAngle
# ═══════════════════════════════════════════════════════════════

class CAR_MODEL(IntEnum):
    """Mapping from KunosID to CarModelId."""
    amr_v12_vantage_gt3 = 12
    audi_r8_lms = 3
    bentley_continental_gt3_2016 = 11
    bentley_continental_gt3_2018 = 8
    bmw_m6_gt3 = 7
    jaguar_g3 = 14
    ferrari_488_gt3 = 2
    honda_nsx_gt3 = 17
    lamborghini_gallardo_rex = 13
    lamborghini_huracan_gt3 = 4
    lamborghini_huracan_st = 18
    lexus_rc_f_gt3 = 15
    mclaren_650s_gt3 = 5
    mercedes_amg_gt3 = 1
    nissan_gt_r_gt3_2017 = 10
    nissan_gt_r_gt3_2018 = 6
    porsche_991_gt3_r = 0
    porsche_991ii_gt3_cup = 9
    amr_v8_vantage_gt3 = 20
    audi_r8_lms_evo = 19
    honda_nsx_gt3_evo = 21
    lamborghini_huracan_gt3_evo = 16
    mclaren_720s_gt3 = 22
    porsche_991ii_gt3_r = 23
    alpine_a110_gt4 = 50
    amr_v8_vantage_gt4 = 51
    audi_r8_gt4 = 52
    bmw_m4_gt4 = 54
    chevrolet_camaro_gt4r = 55
    ginetta_g55_gt4 = 56
    ktm_xbow_gt4 = 57
    maserati_mc_gt4 = 58
    mclaren_570s_gt4 = 59
    mercedes_amg_gt4 = 60
    porsche_718_cayman_gt4_mr = 61
    ferrari_488_gt3_evo = 24
    mercedes_amg_gt3_evo = 25
    ferrari_488_challenge_evo = 26
    bmw_m2_cs_racing = 27
    porsche_992_gt3_cup = 28
    lamborghini_huracan_st_evo2 = 29
    bmw_m4_gt3 = 30
    audi_r8_lms_evo_ii = 31


carModelName = {
    12: "Aston Martin Vantage V12 GT3 2013",
    3: "Audi R8 LMS 2015",
    11: "Bentley Continental GT3 2015",
    8: "Bentley Continental GT3 2018",
    7: "BMW M6 GT3 2017",
    14: "Emil Frey Jaguar G3 2012",
    2: "Ferrari 488 GT3 2018",
    17: "Honda NSX GT3 2017",
    13: "Lamborghini Gallardo G3 Reiter 2017",
    4: "Lamborghini Huracan GT3 2015",
    18: "Lamborghini Huracan ST 2015",
    15: "Lexus RCF GT3 2016",
    5: "McLaren 650S GT3 2015",
    1: "Mercedes AMG GT3 2015",
    10: "Nissan GTR Nismo GT3 2015",
    6: "Nissan GTR Nismo GT3 2018",
    0: "Porsche 991 GT3 R 2018",
    9: "Porsche 991 II GT3 Cup 2017",
    20: "Aston Martin V8 Vantage GT3 2019",
    19: "Audi R8 LMS Evo 2019",
    21: "Honda NSX GT3 Evo 2019",
    16: "Lamborghini Huracan GT3 EVO 2019",
    22: "McLaren 720S GT3 2019",
    23: "Porsche 911 II GT3 R 2019",
    50: "Alpine A110 GT4 2018",
    51: "Aston Martin Vantage AMR GT4 2018",
    52: "Audi R8 LMS GT4 2016",
    53: "BMW M4 GT4 2018",
    55: "Chevrolet Camaro GT4 R 2017",
    56: "Ginetta G55 GT4 2012",
    57: "Ktm Xbow GT4 2016",
    58: "Maserati Gran Turismo MC GT4 2016",
    59: "McLaren 570s GT4 2016",
    60: "Mercedes AMG GT4 2016",
    61: "Porsche 718 Cayman GT4 MR 2019",
    24: "Ferrari 488 GT3 Evo 2020",
    25: "Mercedes AMG GT3 Evo 2020",
    26: "Ferrari 488 Challenge Evo",
    27: "BMW M2 CS Racing",
    28: "Porsche 911 GT3 Cup (Type 992)",
    29: "Lamborghini Huracan Super Trofeo EVO2",
    30: "BMW M4 GT3",
    31: "Audi R8 LMS GT3 evo II",
}
