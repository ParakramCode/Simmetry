"""
Sim Racing Telemetry — Cloud Analytics Dashboard (Enhanced)

Interactive Streamlit dashboard that queries AWS Athena
to visualize telemetry data stored in Amazon S3.

Features:
  - Session overview with multi-lap progression
  - Detailed single-lap telemetry traces
  - Lap-to-lap comparison overlay
  - Braking zone analysis
  - Tyre performance (temp balance, wear, pressure)
  - G-Force friction circle
  - Fuel strategy analysis
  - Electronics usage (TC / ABS / Engine Map)
  - Corner-by-corner sector breakdown

Usage:
    streamlit run src/dashboard/app.py
"""

import json
import streamlit as st
import pandas as pd
import numpy as np
import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots
import sys
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from src.storage.athena_client import AthenaClient

# ─── Track config loader ────────────────────────────────────────────────
TRACK_CONFIG_DIR = Path(__file__).parent.parent.parent / "config" / "tracks"


def load_track_config(track_name: str) -> dict | None:
    """Attempt to load the track JSON config by matching the track name."""
    for json_file in TRACK_CONFIG_DIR.glob("*.json"):
        try:
            cfg = json.loads(json_file.read_text())
            if cfg.get("track_name", "").lower() in track_name.lower() or \
               cfg.get("track_id", "").lower() in track_name.lower():
                return cfg
        except Exception:
            continue
    return None


# ─── Configuration & Theme ──────────────────────────────────────────────
st.set_page_config(
    page_title="SimTelemetry Cloud",
    page_icon="🏎️",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ─── Premium CSS ────────────────────────────────────────────────────────
st.markdown("""
<style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700;800&display=swap');

    /* Global font */
    html, body, [class*="css"] {
        font-family: 'Inter', sans-serif;
    }

    /* Remove default padding */
    .block-container { padding-top: 1.5rem; }

    /* Header bar */
    .dashboard-header {
        background: linear-gradient(135deg, #0f0f23 0%, #1a1a3e 50%, #0d1b2a 100%);
        border: 1px solid rgba(0, 255, 204, 0.15);
        border-radius: 16px;
        padding: 1.5rem 2rem;
        margin-bottom: 1.5rem;
        display: flex;
        align-items: center;
        gap: 1rem;
    }
    .dashboard-header h1 {
        margin: 0;
        font-size: 1.8rem;
        font-weight: 800;
        background: linear-gradient(90deg, #00ffcc, #00bfff, #a78bfa);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        letter-spacing: -0.5px;
    }
    .dashboard-header .subtitle {
        font-size: 0.85rem;
        color: #8892b0;
        margin: 0;
    }

    /* Metric cards */
    .metric-card {
        background: linear-gradient(135deg, rgba(15,15,35,0.8), rgba(26,26,62,0.6));
        border: 1px solid rgba(0,255,204,0.12);
        border-radius: 12px;
        padding: 1.2rem 1rem;
        text-align: center;
        backdrop-filter: blur(12px);
        transition: border-color 0.3s ease, transform 0.2s ease;
    }
    .metric-card:hover {
        border-color: rgba(0,255,204,0.35);
        transform: translateY(-2px);
    }
    .metric-value {
        font-size: 1.9rem;
        font-weight: 700;
        background: linear-gradient(90deg, #00ffcc, #00e5ff);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        line-height: 1.2;
    }
    .metric-value.warn {
        background: linear-gradient(90deg, #f59e0b, #ef4444);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
    }
    .metric-value.good {
        background: linear-gradient(90deg, #34d399, #10b981);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
    }
    .metric-label {
        font-size: 0.72rem;
        color: #8892b0;
        text-transform: uppercase;
        letter-spacing: 1.5px;
        font-weight: 600;
        margin-bottom: 0.3rem;
    }

    /* Section headers */
    .section-header {
        font-size: 1.1rem;
        font-weight: 700;
        color: #e2e8f0;
        padding: 0.5rem 0;
        margin-top: 0.5rem;
        border-left: 3px solid #00ffcc;
        padding-left: 0.75rem;
    }

    /* Tabs styling */
    .stTabs [data-baseweb="tab-list"] {
        gap: 4px;
        background: rgba(15,15,35,0.5);
        border-radius: 12px;
        padding: 4px;
    }
    .stTabs [data-baseweb="tab"] {
        border-radius: 8px;
        padding: 8px 20px;
        font-weight: 600;
        font-size: 0.85rem;
    }

    /* Sidebar styling */
    section[data-testid="stSidebar"] {
        background: linear-gradient(180deg, #0a0a1a 0%, #0f0f23 100%);
        border-right: 1px solid rgba(0,255,204,0.08);
    }
    section[data-testid="stSidebar"] .stSelectbox label,
    section[data-testid="stSidebar"] .stMultiSelect label {
        color: #a78bfa;
        font-weight: 600;
        text-transform: uppercase;
        font-size: 0.72rem;
        letter-spacing: 1px;
    }

    /* Info badges */
    .badge {
        display: inline-block;
        padding: 2px 10px;
        border-radius: 20px;
        font-size: 0.7rem;
        font-weight: 600;
        letter-spacing: 0.5px;
    }
    .badge-cyan { background: rgba(0,255,204,0.15); color: #00ffcc; }
    .badge-purple { background: rgba(167,139,250,0.15); color: #a78bfa; }
    .badge-amber { background: rgba(245,158,11,0.15); color: #f59e0b; }
</style>
""", unsafe_allow_html=True)


# ─── Plotly shared template ─────────────────────────────────────────────
PLOT_TEMPLATE = dict(
    plot_bgcolor="rgba(0,0,0,0)",
    paper_bgcolor="rgba(0,0,0,0)",
    font=dict(family="Inter, sans-serif", color="#a0aec0", size=11),
    legend=dict(
        orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1,
        font=dict(size=10),
    ),
    hovermode="x unified",
    margin=dict(l=30, r=20, t=35, b=30),
)

GRID_COLOR = "rgba(45,55,72,0.5)"
COLORS = {
    "cyan": "#00ffcc", "blue": "#00bfff", "red": "#ef4444",
    "green": "#34d399", "amber": "#f59e0b", "purple": "#a78bfa",
    "orange": "#fb923c", "pink": "#f472b6", "teal": "#2dd4bf",
    "fl": "#60a5fa", "fr": "#34d399", "rl": "#f87171", "rr": "#fbbf24",
}


def _style_axes(fig, rows=1, cols=1, x_title=None):
    """Apply consistent axis styling to every subplot."""
    if rows == 1 and cols == 1:
        fig.update_xaxes(showgrid=True, gridcolor=GRID_COLOR)
        fig.update_yaxes(showgrid=True, gridcolor=GRID_COLOR)
        if x_title:
            fig.update_xaxes(title_text=x_title)
    else:
        for r in range(1, rows + 1):
            for c in range(1, cols + 1):
                fig.update_xaxes(showgrid=True, gridcolor=GRID_COLOR, row=r, col=c)
                fig.update_yaxes(showgrid=True, gridcolor=GRID_COLOR, row=r, col=c)
        if x_title:
            fig.update_xaxes(title_text=x_title, row=rows, col=1)


# ─── Data Source Detection ───────────────────────────────────────────────
# Try Athena first; fall back to local CSV/Parquet when AWS is unreachable.

DATA_ROOT = Path(__file__).parent.parent.parent / "data"
RAW_DATA_ROOT = DATA_ROOT / "raw"
TELEMETRY_ROOT = DATA_ROOT / "telemetry"

_USE_LOCAL: bool | None = None   # resolved once on first call


def _is_local_mode() -> bool:
    """
    Determine data source.  Use local when:
      - DEPLOYMENT_MODE env var is NOT 'aws', OR
      - AthenaClient cannot be instantiated (missing awswrangler, no creds, etc.)
    Falls back to local automatically so the dashboard always loads.
    """
    global _USE_LOCAL
    if _USE_LOCAL is not None:
        return _USE_LOCAL

    import os
    if os.getenv("DEPLOYMENT_MODE", "local") != "aws":
        # Explicitly local or unset → skip the slow AWS connectivity probe
        _USE_LOCAL = True
        return True

    # DEPLOYMENT_MODE=aws → attempt Athena
    try:
        client = AthenaClient()
        client.query("SELECT 1 AS is_online")
        _USE_LOCAL = False
    except Exception:
        _USE_LOCAL = True
    return _USE_LOCAL


# ─── Local-data helpers ─────────────────────────────────────────────────

@st.cache_data
def _load_local_csv() -> pd.DataFrame:
    """Load the first available raw CSV telemetry file."""
    for session_dir in sorted(RAW_DATA_ROOT.iterdir()):
        csv_path = session_dir / "telemetry.csv"
        if csv_path.exists():
            df = pd.read_csv(csv_path)
            df["session_id"] = session_dir.name
            return df
    return pd.DataFrame()


@st.cache_data
def _get_local_sessions() -> pd.DataFrame:
    """Build a sessions DataFrame from whatever local data dirs exist."""
    rows = []
    # Check raw CSV dirs
    if RAW_DATA_ROOT.exists():
        for d in sorted(RAW_DATA_ROOT.iterdir()):
            csv_path = d / "telemetry.csv"
            if csv_path.exists():
                df = pd.read_csv(csv_path, nrows=500)
                rows.append({
                    "session_id": d.name,
                    "car": df["car"].iloc[0] if "car" in df.columns else "Unknown",
                    "track": df["track"].iloc[0] if "track" in df.columns else "Unknown",
                    "session_type": df["session_type"].iloc[0] if "session_type" in df.columns else "practice",
                    "total_laps": int(df["completed_laps"].max()) if "completed_laps" in df.columns else 0,
                    "session_start": df["timestamp_wall"].min() if "timestamp_wall" in df.columns else "",
                    "session_end": df["timestamp_wall"].max() if "timestamp_wall" in df.columns else "",
                    "avg_speed": float(df["speed_kmh"].mean()) if "speed_kmh" in df.columns else 0,
                })
    if not rows:
        return pd.DataFrame()
    return pd.DataFrame(rows)


@st.cache_data
def _get_local_available_laps(session_id: str) -> list[int]:
    """Discover available lap numbers from local data."""
    csv_path = RAW_DATA_ROOT / session_id / "telemetry.csv"
    if csv_path.exists():
        df = pd.read_csv(csv_path, usecols=["completed_laps", "status"])
        df = df[df["status"] == "live"]
        return sorted(df["completed_laps"].dropna().unique().astype(int).tolist())
    return []


@st.cache_data
def _get_local_lap_data(session_id: str, lap: int) -> pd.DataFrame:
    """Load a single lap from local CSV."""
    csv_path = RAW_DATA_ROOT / session_id / "telemetry.csv"
    if csv_path.exists():
        df = pd.read_csv(csv_path)
        df = df[(df["completed_laps"] == lap) & (df["status"] == "live")]
        df = df.sort_values("distance_into_lap")
        return df
    return pd.DataFrame()


@st.cache_data
def _get_local_multi_lap_summary(session_id: str) -> pd.DataFrame:
    """Compute per-lap summary stats from local CSV."""
    csv_path = RAW_DATA_ROOT / session_id / "telemetry.csv"
    if not csv_path.exists():
        return pd.DataFrame()

    df = pd.read_csv(csv_path)
    df = df[(df["status"] == "live") & df["completed_laps"].notna()]

    summary = df.groupby("completed_laps").agg(
        max_speed=("speed_kmh", "max"),
        avg_speed=("speed_kmh", "mean"),
        lap_time_ms=("current_lap_time_ms", "max"),
        avg_temp_fl=("tyre_temp_fl", "mean"),
        avg_temp_fr=("tyre_temp_fr", "mean"),
        avg_temp_rl=("tyre_temp_rl", "mean"),
        avg_temp_rr=("tyre_temp_rr", "mean"),
        avg_fuel=("fuel_remaining", "mean"),
        min_fuel=("fuel_remaining", "min"),
        avg_brake_bias=("brake_bias", "mean"),
        sample_count=("speed_kmh", "count"),
    ).reset_index()
    return summary.sort_values("completed_laps")


# ─── Unified data API (auto-selects Athena vs Local) ────────────────────

@st.cache_resource
def get_athena_client():
    """Cache the client initialization to prevent reconnecting on every render."""
    return AthenaClient()


@st.cache_data(ttl=300)
def fetch_sessions():
    """Fetch all available sessions — Athena or local."""
    if _is_local_mode():
        return _get_local_sessions()
    client = get_athena_client()
    try:
        return client.get_sessions()
    except Exception as e:
        st.error(f"Athena query failed, falling back to local data. Error: {e}")
        return _get_local_sessions()


@st.cache_data(ttl=300)
def fetch_lap_data(session_id: str, lap: int):
    """Fetch 30Hz telemetry data for a specific lap."""
    if _is_local_mode():
        return _get_local_lap_data(session_id, lap)
    client = get_athena_client()
    try:
        return client.get_lap_data(session_id, lap)
    except Exception:
        return _get_local_lap_data(session_id, lap)


@st.cache_data(ttl=300)
def fetch_available_laps(session_id: str):
    """Fetch actual available lap numbers from the data lake."""
    if _is_local_mode():
        return _get_local_available_laps(session_id)
    client = get_athena_client()
    try:
        return client.get_available_laps(session_id)
    except Exception:
        return _get_local_available_laps(session_id)


@st.cache_data(ttl=300)
def fetch_multi_lap_summary(session_id: str):
    """Fetch summary-level stats for all laps in a session."""
    if _is_local_mode():
        return _get_local_multi_lap_summary(session_id)
    client = get_athena_client()
    try:
        return client.query(f"""
            SELECT
                completed_laps,
                MAX(speed_kmh)            AS max_speed,
                AVG(speed_kmh)            AS avg_speed,
                MAX(current_lap_time_ms)  AS lap_time_ms,
                AVG(tyre_temp_fl)         AS avg_temp_fl,
                AVG(tyre_temp_fr)         AS avg_temp_fr,
                AVG(tyre_temp_rl)         AS avg_temp_rl,
                AVG(tyre_temp_rr)         AS avg_temp_rr,
                AVG(fuel_remaining)       AS avg_fuel,
                MIN(fuel_remaining)       AS min_fuel,
                AVG(brake_bias)           AS avg_brake_bias,
                COUNT(*)                  AS sample_count
            FROM telemetry_raw
            WHERE session_id = '{session_id}'
              AND status = 'live'
              AND completed_laps IS NOT NULL
            GROUP BY completed_laps
            ORDER BY completed_laps
        """)
    except Exception:
        return _get_local_multi_lap_summary(session_id)


# ─────────────────────────────────────────────────────────────────────────
# VISUALISATION BUILDERS
# ─────────────────────────────────────────────────────────────────────────

def _fmt_laptime(ms) -> str:
    """Format milliseconds into M:SS.mmm"""
    if pd.isna(ms) or ms <= 0:
        return "--:--.---"
    total_s = ms / 1000
    mins = int(total_s // 60)
    secs = total_s % 60
    return f"{mins}:{secs:06.3f}"


# ── 1. Primary Telemetry Traces ──────────────────────────────────────────
def plot_telemetry_traces(df: pd.DataFrame):
    """Stacked speed / inputs / steering / RPM chart."""
    df = df.sort_values(by="distance_into_lap")
    fig = make_subplots(
        rows=4, cols=1, shared_xaxes=True, vertical_spacing=0.04,
        subplot_titles=("Speed (km/h)", "Throttle & Brake", "Steering Angle", "RPM"),
        row_heights=[0.3, 0.25, 0.2, 0.25],
    )

    fig.add_trace(go.Scatter(
        x=df['distance_into_lap'], y=df['speed_kmh'], name="Speed",
        line=dict(color=COLORS["cyan"], width=2),
        fill='tozeroy', fillcolor="rgba(0,255,229,0.07)",
    ), row=1, col=1)

    fig.add_trace(go.Scatter(
        x=df['distance_into_lap'], y=df['throttle'], name="Throttle",
        line=dict(color=COLORS["green"], width=2),
    ), row=2, col=1)
    fig.add_trace(go.Scatter(
        x=df['distance_into_lap'], y=df['brake'], name="Brake",
        line=dict(color=COLORS["red"], width=2),
    ), row=2, col=1)

    fig.add_trace(go.Scatter(
        x=df['distance_into_lap'], y=df['steering'], name="Steering",
        line=dict(color=COLORS["amber"], width=2),
    ), row=3, col=1)

    fig.add_trace(go.Scatter(
        x=df['distance_into_lap'], y=df['rpm'], name="RPM",
        line=dict(color=COLORS["orange"], width=2),
        fill='tozeroy', fillcolor="rgba(251,146,60,0.06)",
    ), row=4, col=1)

    fig.update_layout(height=780, **PLOT_TEMPLATE)
    _style_axes(fig, rows=4, x_title="Track Distance (m)")
    return fig


# ── 2. Lap Comparison Overlay ────────────────────────────────────────────

# Colour palette for comparison — high contrast, easy to tell apart
_LAP_A_COLOR = "#00ffcc"   # bright cyan (solid)
_LAP_B_COLOR = "#e040fb"   # vivid magenta (dashed)


def _resample_lap(df: pd.DataFrame, n_points: int = 1000) -> pd.DataFrame:
    """
    Re-sample a lap DataFrame onto an evenly-spaced distance grid
    so that two laps with different sample counts align perfectly.
    Uses linear interpolation on every numeric column.
    """
    df = df.sort_values("distance_into_lap").copy()
    d = df["distance_into_lap"].values
    if len(d) < 2:
        return df

    d_min, d_max = float(d.min()), float(d.max())
    common_d = np.linspace(d_min, d_max, n_points)

    out = {"distance_into_lap": common_d}
    for col in df.columns:
        if col == "distance_into_lap":
            continue
        if pd.api.types.is_numeric_dtype(df[col]):
            out[col] = np.interp(common_d, d, df[col].astype(float).values)
    return pd.DataFrame(out)


def plot_lap_comparison(df1: pd.DataFrame, df2: pd.DataFrame, lap1: int, lap2: int):
    """
    Overlay two laps on a common distance grid so every corner
    lines up perfectly.  Includes a speed-delta subplot.
    """
    # Resample both laps onto the same evenly-spaced distance axis
    max_common_dist = min(df1["distance_into_lap"].max(), df2["distance_into_lap"].max())
    n_pts = 1000

    r1 = _resample_lap(df1, n_pts)
    r2 = _resample_lap(df2, n_pts)

    # Trim both to the shorter lap's max distance so lengths match
    r1 = r1[r1["distance_into_lap"] <= max_common_dist].reset_index(drop=True)
    r2 = r2[r2["distance_into_lap"] <= max_common_dist].reset_index(drop=True)

    # Compute speed delta (positive = lap1 faster)
    min_len = min(len(r1), len(r2))
    r1 = r1.iloc[:min_len]
    r2 = r2.iloc[:min_len]
    speed_delta = r1["speed_kmh"].values - r2["speed_kmh"].values

    fig = make_subplots(
        rows=4, cols=1, shared_xaxes=True, vertical_spacing=0.04,
        subplot_titles=(
            f"Speed — Lap {lap1} vs Lap {lap2}",
            f"Speed Delta  (+ = Lap {lap1} faster)",
            "Throttle",
            "Brake",
        ),
        row_heights=[0.30, 0.20, 0.25, 0.25],
    )

    # ── Row 1: Speed overlay ──
    fig.add_trace(go.Scatter(
        x=r1["distance_into_lap"], y=r1["speed_kmh"],
        name=f"Lap {lap1}", line=dict(color=_LAP_A_COLOR, width=2.5),
    ), row=1, col=1)
    fig.add_trace(go.Scatter(
        x=r2["distance_into_lap"], y=r2["speed_kmh"],
        name=f"Lap {lap2}", line=dict(color=_LAP_B_COLOR, width=2.5, dash="dash"),
    ), row=1, col=1)

    # ── Row 2: Speed delta with red/green fill ──
    delta_color = np.where(speed_delta >= 0, "rgba(52,211,153,0.5)", "rgba(239,68,68,0.5)")
    fig.add_trace(go.Bar(
        x=r1["distance_into_lap"], y=speed_delta,
        name="Speed Δ", marker_color=delta_color.tolist(),
        showlegend=False,
    ), row=2, col=1)
    fig.add_hline(y=0, line_color="rgba(255,255,255,0.2)", row=2, col=1)

    # ── Row 3: Throttle overlay ──
    fig.add_trace(go.Scatter(
        x=r1["distance_into_lap"], y=r1["throttle"],
        name=f"Throttle L{lap1}", line=dict(color=_LAP_A_COLOR, width=1.8),
        legendgroup="throttle", showlegend=False,
    ), row=3, col=1)
    fig.add_trace(go.Scatter(
        x=r2["distance_into_lap"], y=r2["throttle"],
        name=f"Throttle L{lap2}", line=dict(color=_LAP_B_COLOR, width=1.8, dash="dash"),
        legendgroup="throttle", showlegend=False,
    ), row=3, col=1)

    # ── Row 4: Brake overlay ──
    fig.add_trace(go.Scatter(
        x=r1["distance_into_lap"], y=r1["brake"],
        name=f"Brake L{lap1}", line=dict(color=_LAP_A_COLOR, width=1.8),
        legendgroup="brake", showlegend=False,
    ), row=4, col=1)
    fig.add_trace(go.Scatter(
        x=r2["distance_into_lap"], y=r2["brake"],
        name=f"Brake L{lap2}", line=dict(color=_LAP_B_COLOR, width=1.8, dash="dash"),
        legendgroup="brake", showlegend=False,
    ), row=4, col=1)

    fig.update_layout(height=820, **PLOT_TEMPLATE)
    fig.update_yaxes(title_text="km/h", row=1, col=1)
    fig.update_yaxes(title_text="Δ km/h", row=2, col=1)
    fig.update_yaxes(title_text="Input", row=3, col=1)
    fig.update_yaxes(title_text="Input", row=4, col=1)
    _style_axes(fig, rows=4, x_title="Track Distance (m)")
    return fig


# ── 3. Braking Analysis ─────────────────────────────────────────────────
def plot_braking_analysis(df: pd.DataFrame):
    """Brake pressure heatmap + brake temp chart."""
    df = df.sort_values("distance_into_lap")

    fig = make_subplots(
        rows=2, cols=1, shared_xaxes=True, vertical_spacing=0.08,
        subplot_titles=("Brake Pressure & Heavy Braking Zones", "Brake Disc Temperatures"),
        row_heights=[0.5, 0.5],
    )

    # Brake input with heavy-braking shading
    fig.add_trace(go.Scatter(
        x=df['distance_into_lap'], y=df['brake'], name="Brake",
        line=dict(color=COLORS["red"], width=2),
        fill='tozeroy', fillcolor="rgba(239,68,68,0.12)",
    ), row=1, col=1)

    # Add a threshold line at 0.8
    fig.add_hline(y=0.8, line_dash="dash", line_color=COLORS["amber"],
                  annotation_text="Heavy brake threshold", row=1, col=1)

    # Speed mini overlay (secondary axis feel)
    fig.add_trace(go.Scatter(
        x=df['distance_into_lap'], y=df['speed_kmh'] / df['speed_kmh'].max(),
        name="Speed (normalized)", line=dict(color=COLORS["cyan"], width=1, dash="dot"),
        opacity=0.4,
    ), row=1, col=1)

    # Brake temps
    if 'brake_temp_fl' in df.columns:
        for corner, color in COLORS.items():
            if corner in ("fl", "fr", "rl", "rr"):
                col_name = f"brake_temp_{corner}"
                if col_name in df.columns:
                    fig.add_trace(go.Scatter(
                        x=df['distance_into_lap'], y=df[col_name],
                        name=f"Brake Temp {corner.upper()}",
                        line=dict(color=color, width=1.5),
                    ), row=2, col=1)

    fig.update_layout(height=550, **PLOT_TEMPLATE)
    _style_axes(fig, rows=2, x_title="Track Distance (m)")
    return fig


# ── 4. Tyre Performance ─────────────────────────────────────────────────
def plot_tyre_temps(df: pd.DataFrame):
    """Core tyre temps across the lap per corner."""
    df = df.sort_values("distance_into_lap")
    fig = go.Figure()
    for corner in ("fl", "fr", "rl", "rr"):
        col = f"tyre_temp_{corner}"
        if col in df.columns:
            fig.add_trace(go.Scatter(
                x=df['distance_into_lap'], y=df[col],
                name=f"{corner.upper()}", line=dict(color=COLORS[corner], width=2),
            ))

    fig.update_layout(
        title="Core Tyre Temperatures vs Distance",
        xaxis_title="Track Distance (m)", yaxis_title="Temperature (°C)",
        height=320, **PLOT_TEMPLATE,
    )
    _style_axes(fig)
    return fig


def plot_tyre_pressures(df: pd.DataFrame):
    """Tyre pressures across the lap."""
    df = df.sort_values("distance_into_lap")
    fig = go.Figure()
    for corner in ("fl", "fr", "rl", "rr"):
        col = f"tyre_pressure_{corner}"
        if col in df.columns:
            fig.add_trace(go.Scatter(
                x=df['distance_into_lap'], y=df[col],
                name=f"{corner.upper()}", line=dict(color=COLORS[corner], width=2),
            ))

    fig.update_layout(
        title="Tyre Pressures vs Distance",
        xaxis_title="Track Distance (m)", yaxis_title="Pressure (psi)",
        height=320, **PLOT_TEMPLATE,
    )
    _style_axes(fig)
    return fig


def plot_tyre_wear(df: pd.DataFrame):
    """Tyre wear across the lap (if column exists in data)."""
    df = df.sort_values("distance_into_lap")
    wear_cols = [c for c in df.columns if "tyre_wear" in c]
    if not wear_cols:
        return None
    fig = go.Figure()
    for corner in ("fl", "fr", "rl", "rr"):
        col = f"tyre_wear_{corner}"
        if col in df.columns:
            fig.add_trace(go.Scatter(
                x=df['distance_into_lap'], y=df[col],
                name=f"{corner.upper()}", line=dict(color=COLORS[corner], width=2),
            ))
    fig.update_layout(
        title="Tyre Wear Progression",
        xaxis_title="Track Distance (m)", yaxis_title="Wear (0 = new)",
        height=320, **PLOT_TEMPLATE,
    )
    _style_axes(fig)
    return fig


def render_tyre_balance_heatmap(df: pd.DataFrame):
    """Render a 2x2 average-temp heatmap representing the car's tyre balance."""
    avgs = {}
    for corner in ("fl", "fr", "rl", "rr"):
        col = f"tyre_temp_{corner}"
        if col in df.columns:
            avgs[corner] = round(float(df[col].mean()), 1)
        else:
            avgs[corner] = 0.0

    matrix = [
        [avgs.get("fl", 0), avgs.get("fr", 0)],
        [avgs.get("rl", 0), avgs.get("rr", 0)],
    ]
    labels = [["FL", "FR"], ["RL", "RR"]]
    text_vals = [[f"{v}°C" for v in row] for row in matrix]

    fig = go.Figure(data=go.Heatmap(
        z=matrix, text=text_vals, texttemplate="%{text}",
        x=["Left", "Right"], y=["Front", "Rear"],
        colorscale=[[0, "#0d1b2a"], [0.5, "#f59e0b"], [1, "#ef4444"]],
        showscale=True, colorbar=dict(title="°C"),
    ))
    fig.update_layout(
        title="Avg Tyre Temperature Balance",
        height=280, width=320,
        **{k: v for k, v in PLOT_TEMPLATE.items() if k not in ("hovermode",)},
        yaxis=dict(autorange="reversed"),
    )
    return fig


# ── 5. G-Force Friction Circle ──────────────────────────────────────────
def plot_gforce_scatter(df: pd.DataFrame):
    """Lat vs Lon g-force scatter coloured by speed."""
    if 'g_lat' not in df.columns or 'g_lon' not in df.columns:
        return None

    fig = go.Figure(data=go.Scattergl(
        x=df['g_lat'], y=df['g_lon'],
        mode='markers',
        marker=dict(
            size=3, color=df['speed_kmh'],
            colorscale=[[0, "#0d1b2a"], [0.3, "#00bfff"], [0.6, "#00ffcc"], [1, "#ef4444"]],
            colorbar=dict(title="km/h"),
            opacity=0.65,
        ),
        hovertemplate="Lat G: %{x:.2f}<br>Lon G: %{y:.2f}<extra></extra>",
    ))

    # Reference circles
    for r in (1.0, 1.5, 2.0):
        theta = np.linspace(0, 2 * np.pi, 100)
        fig.add_trace(go.Scatter(
            x=r * np.cos(theta), y=r * np.sin(theta),
            mode='lines', line=dict(color="rgba(255,255,255,0.08)", width=1),
            showlegend=False, hoverinfo='skip',
        ))

    fig.update_layout(
        title="G-Force Friction Circle",
        xaxis_title="Lateral G", yaxis_title="Longitudinal G",
        xaxis=dict(scaleanchor="y", scaleratio=1),
        height=450, **PLOT_TEMPLATE,
    )
    _style_axes(fig)
    return fig


# ── 6. Fuel Strategy ────────────────────────────────────────────────────
def plot_fuel_analysis(df: pd.DataFrame, summary_df: pd.DataFrame = None):
    """Fuel remaining across the lap + session fuel burn trend."""
    df = df.sort_values("distance_into_lap")

    fig = make_subplots(
        rows=1, cols=2, horizontal_spacing=0.08,
        subplot_titles=("Fuel Level During Lap", "Fuel Burn Per Lap (Session)"),
    )

    fig.add_trace(go.Scatter(
        x=df['distance_into_lap'], y=df['fuel_remaining'],
        name="Fuel Level", line=dict(color=COLORS["amber"], width=2),
        fill='tozeroy', fillcolor="rgba(245,158,11,0.08)",
    ), row=1, col=1)

    if summary_df is not None and not summary_df.empty and 'avg_fuel' in summary_df.columns:
        burn = summary_df['avg_fuel'].diff().abs().fillna(0)
        fig.add_trace(go.Bar(
            x=summary_df['completed_laps'], y=burn,
            name="Fuel Burned", marker_color=COLORS["orange"],
            opacity=0.8,
        ), row=1, col=2)

    fig.update_layout(height=350, **PLOT_TEMPLATE)
    _style_axes(fig, rows=1, cols=2)
    fig.update_xaxes(title_text="Distance (m)", row=1, col=1)
    fig.update_xaxes(title_text="Lap Number", row=1, col=2)
    fig.update_yaxes(title_text="Litres", row=1, col=1)
    return fig


# ── 7. Electronics Usage ────────────────────────────────────────────────
def plot_electronics(df: pd.DataFrame):
    """TC level, ABS level, and engine map across the lap."""
    df = df.sort_values("distance_into_lap")
    elec_cols = {"tc_level": "TC Level", "abs_level": "ABS Level", "engine_map": "Engine Map"}
    available = {k: v for k, v in elec_cols.items() if k in df.columns}
    if not available:
        return None

    fig = make_subplots(
        rows=len(available), cols=1, shared_xaxes=True,
        vertical_spacing=0.06,
        subplot_titles=list(available.values()),
    )
    palette = [COLORS["cyan"], COLORS["purple"], COLORS["amber"]]
    for i, (col, label) in enumerate(available.items(), 1):
        fig.add_trace(go.Scatter(
            x=df['distance_into_lap'], y=df[col],
            name=label, line=dict(color=palette[i - 1], width=2),
            mode='lines',
        ), row=i, col=1)

    fig.update_layout(height=max(250, len(available) * 180), **PLOT_TEMPLATE)
    _style_axes(fig, rows=len(available), x_title="Track Distance (m)")
    return fig


# ── 8. Session Lap-Time Progression ─────────────────────────────────────
def plot_lap_progression(summary_df: pd.DataFrame):
    """Bar chart of lap times across the session."""
    if summary_df.empty or 'lap_time_ms' not in summary_df.columns:
        return None

    valid = summary_df[summary_df['lap_time_ms'] > 0].copy()
    if valid.empty:
        return None

    valid['lap_time_s'] = valid['lap_time_ms'] / 1000
    valid['label'] = valid['lap_time_ms'].apply(_fmt_laptime)

    best_idx = valid['lap_time_s'].idxmin()
    colors = [COLORS["purple"] if i != best_idx else COLORS["cyan"] for i in valid.index]

    fig = go.Figure(data=go.Bar(
        x=valid['completed_laps'], y=valid['lap_time_s'],
        text=valid['label'], textposition='outside',
        marker_color=colors, opacity=0.9,
    ))
    fig.update_layout(
        title="Lap Time Progression",
        xaxis_title="Lap", yaxis_title="Time (s)",
        height=350, **PLOT_TEMPLATE,
    )
    _style_axes(fig)
    return fig


# ── 9. Corner-by-Corner Analysis ────────────────────────────────────────
def plot_corner_speeds(df: pd.DataFrame, track_cfg: dict):
    """Analyse min speed at each named corner vs reference range."""
    corners = track_cfg.get("corners", {})
    if not corners:
        return None

    df = df.sort_values("distance_into_lap")
    rows = []
    for name, info in corners.items():
        entry = info.get("entry_m", 0)
        exit_ = info.get("exit_m", 0)
        mask = (df['distance_into_lap'] >= entry) & (df['distance_into_lap'] <= exit_)
        segment = df[mask]
        if segment.empty:
            continue
        ref = info.get("typical_speed_range_kmh", [0, 0])
        rows.append(dict(
            corner=name.replace("_", " "),
            min_speed=round(float(segment['speed_kmh'].min()), 1),
            avg_speed=round(float(segment['speed_kmh'].mean()), 1),
            ref_low=ref[0],
            ref_high=ref[1],
        ))

    if not rows:
        return None

    cdf = pd.DataFrame(rows)

    fig = go.Figure()
    fig.add_trace(go.Bar(
        x=cdf['corner'], y=cdf['min_speed'],
        name="Your Min Speed", marker_color=COLORS["cyan"], opacity=0.85,
    ))
    fig.add_trace(go.Scatter(
        x=cdf['corner'], y=cdf['ref_low'],
        mode='markers+lines', name="Ref Low", line=dict(dash="dot", color=COLORS["amber"]),
    ))
    fig.add_trace(go.Scatter(
        x=cdf['corner'], y=cdf['ref_high'],
        mode='markers+lines', name="Ref High", line=dict(dash="dot", color=COLORS["green"]),
    ))

    fig.update_layout(
        title="Corner Minimum Speed vs Reference",
        xaxis_title="Corner", yaxis_title="Speed (km/h)",
        height=400, barmode='group', **PLOT_TEMPLATE,
    )
    _style_axes(fig)
    return fig


# ─────────────────────────────────────────────────────────────────────────
# APP LAYOUT
# ─────────────────────────────────────────────────────────────────────────

def main():
    # Header
    st.markdown("""
    <div class="dashboard-header">
        <div>
            <h1>🏎️ SimTelemetry Cloud Dashboard</h1>
            <p class="subtitle">Real-time telemetry analytics powered by AWS Athena &bull; S3 Data Lake</p>
        </div>
    </div>
    """, unsafe_allow_html=True)

    # Load sessions
    sessions_df = fetch_sessions()
    if sessions_df.empty:
        st.warning("No live sessions found in AWS Athena Data Lake. "
                    "Ensure Firehose is streaming and data is available.")
        return

    # ── Sidebar ──────────────────────────────────────────────────────────
    with st.sidebar:
        st.markdown("### ⚙️ Session Controls")

        session_opts = sessions_df['session_id'].tolist()
        selected_session = st.selectbox("Session", session_opts)
        session_meta = sessions_df[sessions_df['session_id'] == selected_session].iloc[0]

        st.markdown("---")
        col_a, col_b = st.columns(2)
        col_a.markdown(f'<span class="badge badge-cyan">{session_meta["track"]}</span>', unsafe_allow_html=True)
        col_b.markdown(f'<span class="badge badge-purple">{session_meta["car"]}</span>', unsafe_allow_html=True)
        st.markdown(f'<span class="badge badge-amber">{session_meta["total_laps"]} laps recorded</span>',
                    unsafe_allow_html=True)
        st.markdown("---")

        with st.spinner("Fetching laps..."):
            lap_options = fetch_available_laps(selected_session)

        if not lap_options:
            st.warning("No lap data available for this session.")
            return

        selected_lap = st.selectbox("Primary Lap", lap_options)

        # Comparison lap
        st.markdown("---")
        st.markdown("### 🔄 Lap Comparison")
        compare_enabled = st.checkbox("Enable lap overlay", value=False)
        compare_lap = None
        if compare_enabled:
            other_laps = [l for l in lap_options if l != selected_lap]
            if other_laps:
                compare_lap = st.selectbox("Compare Against", other_laps)
            else:
                st.info("Need at least 2 laps for comparison.")

        # Cache buster
        st.markdown("---")
        if st.button("🔄 Refresh Data", use_container_width=True):
            st.cache_data.clear()
            st.rerun()

    # ── Fetch data ───────────────────────────────────────────────────────
    with st.spinner(f"Querying Athena for Lap {selected_lap}..."):
        lap_df = fetch_lap_data(selected_session, selected_lap)

    if lap_df.empty:
        st.warning(f"No valid telemetry data found for Lap {selected_lap}.")
        return

    with st.spinner("Loading session summary..."):
        summary_df = fetch_multi_lap_summary(selected_session)

    track_cfg = load_track_config(str(session_meta.get("track", "")))

    compare_df = None
    if compare_enabled and compare_lap is not None:
        with st.spinner(f"Loading Lap {compare_lap} for comparison..."):
            compare_df = fetch_lap_data(selected_session, compare_lap)

    # ── Top Metrics Row ──────────────────────────────────────────────────
    m1, m2, m3, m4, m5, m6 = st.columns(6)

    max_speed = lap_df['speed_kmh'].max()
    avg_speed = lap_df['speed_kmh'].mean()
    lap_time_ms = int(lap_df['current_lap_time_ms'].max()) if 'current_lap_time_ms' in lap_df.columns else 0
    fuel_used = lap_df['fuel_remaining'].max() - lap_df['fuel_remaining'].min() if 'fuel_remaining' in lap_df.columns else 0
    max_g = np.sqrt(lap_df['g_lat'] ** 2 + lap_df['g_lon'] ** 2).max() if 'g_lat' in lap_df.columns else 0
    avg_tyre_temp = pd.concat([
        lap_df.get(f'tyre_temp_{c}', pd.Series(dtype=float)) for c in ("fl", "fr", "rl", "rr")
    ]).mean()

    def _card(col, label, value, cls=""):
        col.markdown(f"""
        <div class="metric-card">
            <div class="metric-label">{label}</div>
            <div class="metric-value {cls}">{value}</div>
        </div>
        """, unsafe_allow_html=True)

    _card(m1, "Max Speed", f"{max_speed:.1f} km/h")
    _card(m2, "Avg Speed", f"{avg_speed:.1f} km/h")
    _card(m3, "Lap Time", _fmt_laptime(lap_time_ms))
    _card(m4, "Fuel Used", f"{fuel_used:.2f} L", "warn" if fuel_used > 3.5 else "")
    _card(m5, "Peak G", f"{max_g:.2f} G")
    _card(m6, "Avg Tyre Temp", f"{avg_tyre_temp:.0f}°C",
          "warn" if avg_tyre_temp > 100 else ("good" if avg_tyre_temp > 75 else ""))

    st.markdown("")

    # ── Tabs ─────────────────────────────────────────────────────────────
    tab_names = [
        "📊 Telemetry",
        "🏁 Session Overview",
        "🔄 Lap Compare",
        "🛞 Tyres",
        "🛑 Braking",
        "🎯 G-Force",
        "⛽ Fuel",
        "🔧 Electronics",
    ]
    if track_cfg:
        tab_names.append("🗺️ Corner Analysis")

    tabs = st.tabs(tab_names)

    # ── Tab 1: Primary Telemetry ─────────────────────────────────────────
    with tabs[0]:
        st.markdown('<div class="section-header">Lap Telemetry Traces</div>', unsafe_allow_html=True)
        st.plotly_chart(plot_telemetry_traces(lap_df), use_container_width=True)

    # ── Tab 2: Session Overview ──────────────────────────────────────────
    with tabs[1]:
        st.markdown('<div class="section-header">Session Summary</div>', unsafe_allow_html=True)

        if not summary_df.empty:
            prog_fig = plot_lap_progression(summary_df)
            if prog_fig:
                st.plotly_chart(prog_fig, use_container_width=True)

            # Summary table
            st.markdown('<div class="section-header">Lap-by-Lap Statistics</div>', unsafe_allow_html=True)
            display_df = summary_df.copy()
            display_df['lap_time'] = display_df['lap_time_ms'].apply(_fmt_laptime)
            display_df['avg_temp'] = (
                (display_df['avg_temp_fl'] + display_df['avg_temp_fr'] +
                 display_df['avg_temp_rl'] + display_df['avg_temp_rr']) / 4
            ).round(1)

            show_cols = ['completed_laps', 'lap_time', 'max_speed', 'avg_speed',
                         'avg_temp', 'avg_fuel', 'sample_count']
            available_cols = [c for c in show_cols if c in display_df.columns]
            st.dataframe(
                display_df[available_cols].rename(columns={
                    'completed_laps': 'Lap', 'lap_time': 'Time', 'max_speed': 'Max Spd',
                    'avg_speed': 'Avg Spd', 'avg_temp': 'Avg Tyre °C',
                    'avg_fuel': 'Fuel (L)', 'sample_count': 'Samples',
                }),
                use_container_width=True, hide_index=True,
            )

            # Speed trend across laps
            if 'max_speed' in summary_df.columns and 'avg_speed' in summary_df.columns:
                trend_fig = go.Figure()
                trend_fig.add_trace(go.Scatter(
                    x=summary_df['completed_laps'], y=summary_df['max_speed'],
                    name="Max Speed", line=dict(color=COLORS["cyan"], width=2),
                    mode='lines+markers',
                ))
                trend_fig.add_trace(go.Scatter(
                    x=summary_df['completed_laps'], y=summary_df['avg_speed'],
                    name="Avg Speed", line=dict(color=COLORS["purple"], width=2),
                    mode='lines+markers',
                ))
                trend_fig.update_layout(
                    title="Speed Trend Across Laps",
                    xaxis_title="Lap", yaxis_title="km/h",
                    height=320, **PLOT_TEMPLATE,
                )
                _style_axes(trend_fig)
                st.plotly_chart(trend_fig, use_container_width=True)
        else:
            st.info("No multi-lap summary data available.")

    # ── Tab 3: Lap Comparison ────────────────────────────────────────────
    with tabs[2]:
        if compare_df is not None and not compare_df.empty:
            st.markdown(
                f'<div class="section-header">Lap {selected_lap} vs Lap {compare_lap}</div>',
                unsafe_allow_html=True,
            )

            # Delta metrics
            d1, d2, d3, d4 = st.columns(4)
            ms1 = lap_df['speed_kmh'].max()
            ms2 = compare_df['speed_kmh'].max()
            as1 = lap_df['speed_kmh'].mean()
            as2 = compare_df['speed_kmh'].mean()
            lt1 = int(lap_df['current_lap_time_ms'].max()) if 'current_lap_time_ms' in lap_df.columns else 0
            lt2 = int(compare_df['current_lap_time_ms'].max()) if 'current_lap_time_ms' in compare_df.columns else 0
            delta_ms = lt1 - lt2

            _card(d1, f"Max Speed L{selected_lap}", f"{ms1:.1f}")
            _card(d2, f"Max Speed L{compare_lap}", f"{ms2:.1f}")
            _card(d3, f"Time L{selected_lap}", _fmt_laptime(lt1))
            _card(d4, "Delta", f"{'+' if delta_ms > 0 else ''}{delta_ms / 1000:.3f}s",
                  "warn" if delta_ms > 0 else "good")

            st.markdown("")
            st.plotly_chart(
                plot_lap_comparison(lap_df, compare_df, selected_lap, compare_lap),
                use_container_width=True,
            )
        else:
            st.info("Enable **Lap Comparison** in the sidebar and select a second lap to overlay.")

    # ── Tab 4: Tyres ─────────────────────────────────────────────────────
    with tabs[3]:
        st.markdown('<div class="section-header">Tyre Performance Analysis</div>', unsafe_allow_html=True)

        tc1, tc2 = st.columns([2, 1])
        with tc1:
            st.plotly_chart(plot_tyre_temps(lap_df), use_container_width=True)
            st.plotly_chart(plot_tyre_pressures(lap_df), use_container_width=True)
            wear_fig = plot_tyre_wear(lap_df)
            if wear_fig:
                st.plotly_chart(wear_fig, use_container_width=True)
        with tc2:
            st.plotly_chart(render_tyre_balance_heatmap(lap_df), use_container_width=True)

            # Quick stats
            st.markdown('<div class="section-header">Tyre Stats</div>', unsafe_allow_html=True)
            for corner in ("fl", "fr", "rl", "rr"):
                temp_col = f"tyre_temp_{corner}"
                pres_col = f"tyre_pressure_{corner}"
                if temp_col in lap_df.columns and pres_col in lap_df.columns:
                    avg_t = lap_df[temp_col].mean()
                    avg_p = lap_df[pres_col].mean()
                    st.markdown(f"**{corner.upper()}** — {avg_t:.1f}°C / {avg_p:.1f} psi")

    # ── Tab 5: Braking ───────────────────────────────────────────────────
    with tabs[4]:
        st.markdown('<div class="section-header">Braking Zone Analysis</div>', unsafe_allow_html=True)
        st.plotly_chart(plot_braking_analysis(lap_df), use_container_width=True)

        # Braking stats
        if 'brake' in lap_df.columns:
            heavy = (lap_df['brake'] > 0.8).sum()
            total = len(lap_df)
            pct = (heavy / total * 100) if total > 0 else 0
            bs1, bs2, bs3 = st.columns(3)
            _card(bs1, "Heavy Brake Samples", f"{heavy}")
            _card(bs2, "% of Lap", f"{pct:.1f}%")
            _card(bs3, "Max Brake Pressure", f"{lap_df['brake'].max():.2f}")

    # ── Tab 6: G-Force ───────────────────────────────────────────────────
    with tabs[5]:
        st.markdown('<div class="section-header">G-Force Friction Circle</div>', unsafe_allow_html=True)
        gf_fig = plot_gforce_scatter(lap_df)
        if gf_fig:
            gc1, gc2 = st.columns([3, 1])
            with gc1:
                st.plotly_chart(gf_fig, use_container_width=True)
            with gc2:
                st.markdown('<div class="section-header">G-Force Stats</div>', unsafe_allow_html=True)
                if 'g_lat' in lap_df.columns:
                    _card(gc2, "Max Lat G", f"{lap_df['g_lat'].abs().max():.2f}")
                if 'g_lon' in lap_df.columns:
                    _card(gc2, "Max Lon G (Braking)", f"{lap_df['g_lon'].min():.2f}")
                    _card(gc2, "Max Lon G (Accel)", f"{lap_df['g_lon'].max():.2f}")
        else:
            st.info("G-Force data not available in this lap's telemetry.")

    # ── Tab 7: Fuel ──────────────────────────────────────────────────────
    with tabs[6]:
        st.markdown('<div class="section-header">Fuel Strategy</div>', unsafe_allow_html=True)
        st.plotly_chart(plot_fuel_analysis(lap_df, summary_df), use_container_width=True)

        if 'fuel_remaining' in lap_df.columns and 'fuel_per_lap' in lap_df.columns:
            fuel_rem = lap_df['fuel_remaining'].iloc[-1]
            fpl = lap_df['fuel_per_lap'].mean()
            est_laps = fuel_rem / fpl if fpl > 0 else 0
            f1, f2, f3 = st.columns(3)
            _card(f1, "Fuel Remaining", f"{fuel_rem:.2f} L")
            _card(f2, "Per-Lap Burn", f"{fpl:.2f} L")
            _card(f3, "Est. Laps Left", f"{est_laps:.1f}", "warn" if est_laps < 3 else "good")

    # ── Tab 8: Electronics ───────────────────────────────────────────────
    with tabs[7]:
        st.markdown('<div class="section-header">Electronics Usage</div>', unsafe_allow_html=True)
        elec_fig = plot_electronics(lap_df)
        if elec_fig:
            st.plotly_chart(elec_fig, use_container_width=True)

            # Summary cards
            e1, e2, e3 = st.columns(3)
            if 'tc_level' in lap_df.columns:
                _card(e1, "Avg TC Level", f"{lap_df['tc_level'].mean():.1f}")
            if 'abs_level' in lap_df.columns:
                _card(e2, "Avg ABS Level", f"{lap_df['abs_level'].mean():.1f}")
            if 'brake_bias' in lap_df.columns:
                _card(e3, "Avg Brake Bias", f"{lap_df['brake_bias'].mean():.1f}%")
        else:
            st.info("Electronics data not available.")

    # ── Tab 9 (optional): Corner Analysis ────────────────────────────────
    if track_cfg and len(tabs) > 8:
        with tabs[8]:
            st.markdown(
                f'<div class="section-header">Corner Analysis — {track_cfg.get("track_name", "")}</div>',
                unsafe_allow_html=True,
            )
            corner_fig = plot_corner_speeds(lap_df, track_cfg)
            if corner_fig:
                st.plotly_chart(corner_fig, use_container_width=True)

                # Corner table
                corners = track_cfg.get("corners", {})
                df_sorted = lap_df.sort_values("distance_into_lap")
                corner_rows = []
                for name, info in corners.items():
                    entry, exit_ = info.get("entry_m", 0), info.get("exit_m", 0)
                    mask = (df_sorted['distance_into_lap'] >= entry) & (df_sorted['distance_into_lap'] <= exit_)
                    seg = df_sorted[mask]
                    if seg.empty:
                        continue
                    ref = info.get("typical_speed_range_kmh", [0, 0])
                    min_spd = seg['speed_kmh'].min()
                    corner_rows.append({
                        "Corner": name.replace("_", " "),
                        "Min Speed": f"{min_spd:.1f}",
                        "Avg Speed": f"{seg['speed_kmh'].mean():.1f}",
                        "Ref Range": f"{ref[0]}–{ref[1]}",
                        "Delta to Ref Low": f"{min_spd - ref[0]:+.1f}",
                    })
                if corner_rows:
                    st.dataframe(pd.DataFrame(corner_rows), use_container_width=True, hide_index=True)
            else:
                st.info("Could not compute corner analysis — check distance data.")


if __name__ == "__main__":
    main()
