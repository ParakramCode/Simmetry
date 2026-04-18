"""
Sim Racing Telemetry — Cloud Analytics Dashboard

Interactive Streamlit dashboard that queries AWS Athena
to visualize telemetry data stored in Amazon S3.

Usage:
    streamlit run src/dashboard/app.py
"""

import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import sys
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from src.storage.athena_client import AthenaClient

# ─── Configuration & Theme ──────────────────────────────────────────────
st.set_page_config(
    page_title="SimTelemetry Cloud",
    page_icon="🏎️",
    layout="wide",
    initial_sidebar_state="expanded",
)

# Custom CSS for dark mode styling
st.markdown("""
<style>
    .metric-value { 
        font-size: 2rem; 
        font-weight: 700; 
        color: #00ffcc; 
    }
    .metric-label { 
        font-size: 0.9rem; 
        color: #a0aec0; 
        text-transform: uppercase; 
        letter-spacing: 1px;
    }
    .dashboard-header { 
        margin-bottom: 2rem; 
        padding-bottom: 1rem; 
        border-bottom: 1px solid #2d3748;
    }
</style>
""", unsafe_allow_html=True)


# ─── Data Loading (Cached) ──────────────────────────────────────────────
@st.cache_resource
def get_athena_client():
    """Cache the client initialization to prevent reconnecting on every render."""
    return AthenaClient()


@st.cache_data(ttl=300)  # Cache for 5 mins to limit Athena API usage costs
def fetch_sessions():
    """Fetch all available sessions."""
    client = get_athena_client()
    try:
        return client.get_sessions()
    except Exception as e:
        st.error(f"Failed to fetch sessions from Athena. Ensure 'setup_aws.py' tables were created. Error: {e}")
        return pd.DataFrame()


@st.cache_data(ttl=300)
def fetch_lap_data(session_id: str, lap: int):
    """Fetch 60Hz telemetry data for a specific lap."""
    client = get_athena_client()
    return client.get_lap_data(session_id, lap)

@st.cache_data(ttl=300)
def fetch_available_laps(session_id: str):
    """Fetch actual available lap numbers from the data lake."""
    client = get_athena_client()
    return client.get_available_laps(session_id)


# ─── Visualization Generators ───────────────────────────────────────────
def plot_telemetry_traces(df: pd.DataFrame):
    """Generate a stacked Plotly chart for telemetry traces."""
    # Ensure distance is sorted
    df = df.sort_values(by="distance_into_lap")

    fig = make_subplots(
        rows=4, cols=1,
        shared_xaxes=True,
        vertical_spacing=0.03,
        subplot_titles=("Speed (km/h)", "Inputs (Throttle & Brake)", "Steering Angle", "Gear & RPM"),
        row_heights=[0.3, 0.3, 0.2, 0.2]
    )

    # 1. Speed Trace
    fig.add_trace(
        go.Scatter(
            x=df['distance_into_lap'], y=df['speed_kmh'],
            name="Speed", line=dict(color="#00ffe5", width=2),
            fill='tozeroy', fillcolor="rgba(0, 255, 229, 0.1)"
        ),
        row=1, col=1
    )

    # 2. Inputs (Throttle vs Brake)
    fig.add_trace(
        go.Scatter(
            x=df['distance_into_lap'], y=df['throttle'],
            name="Throttle", line=dict(color="#38a169", width=2)
        ),
        row=2, col=1
    )
    fig.add_trace(
        go.Scatter(
            x=df['distance_into_lap'], y=df['brake'],
            name="Brake", line=dict(color="#e53e3e", width=2)
        ),
        row=2, col=1
    )

    # 3. Steering
    fig.add_trace(
        go.Scatter(
            x=df['distance_into_lap'], y=df['steering'],
            name="Steering", line=dict(color="#ecc94b", width=2)
        ),
        row=3, col=1
    )

    # 4. Engine (RPM)
    fig.add_trace(
        go.Scatter(
            x=df['distance_into_lap'], y=df['rpm'],
            name="RPM", line=dict(color="#ed8936", width=2)
        ),
        row=4, col=1
    )

    # Styling
    fig.update_layout(
        height=800,
        margin=dict(l=20, r=20, t=40, b=20),
        plot_bgcolor="rgba(0,0,0,0)",
        paper_bgcolor="rgba(0,0,0,0)",
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        hovermode="x unified"
    )

    # Axis styling
    fig.update_xaxes(title_text="Track Distance (m)", row=4, col=1, showgrid=True, gridcolor="#2d3748")
    for i in range(1, 5):
        fig.update_yaxes(showgrid=True, gridcolor="#2d3748", row=i, col=1)

    return fig


def plot_tyre_temps(df: pd.DataFrame):
    """Plot tyre temperatures across the lap."""
    df = df.sort_values(by="distance_into_lap")
    
    fig = go.Figure()
    
    colors = {"fl": "#4299e1", "fr": "#4fd1c5", "rl": "#f56565", "rr": "#ed8936"}
    
    for corner, color in colors.items():
        fig.add_trace(
            go.Scatter(
                x=df['distance_into_lap'], y=df[f'tyre_temp_{corner}'],
                name=f"Temp {corner.upper()}", line=dict(color=color, width=2)
            )
        )
        
    fig.update_layout(
        title="Core Tyre Temps vs Distance",
        xaxis_title="Track Distance (m)",
        yaxis_title="Temperature (°C)",
        height=300,
        margin=dict(l=20, r=20, t=40, b=20),
        plot_bgcolor="rgba(0,0,0,0)",
        paper_bgcolor="rgba(0,0,0,0)",
        hovermode="x unified"
    )
    fig.update_xaxes(showgrid=True, gridcolor="#2d3748")
    fig.update_yaxes(showgrid=True, gridcolor="#2d3748")
    
    return fig


# ─── App Layout ─────────────────────────────────────────────────────────
def main():
    st.markdown('<div class="dashboard-header"><h1>🏎️ SimTelemetry Cloud Dashboard</h1></div>', unsafe_allow_html=True)

    # Load sessions safely
    sessions_df = fetch_sessions()
    
    if sessions_df.empty:
        st.warning("No live sessions found in AWS Athena Data Lake. Ensure Firehose is streaming and data is available.")
        return

    # Sidebar: Session & Lap Selection
    with st.sidebar:
        st.header("⚙️ Data Filters")
        
        # Session dropdown
        session_opts = sessions_df['session_id'].tolist()
        selected_session = st.selectbox("Select Session", session_opts)
        
        # Get metadata for the selected session
        session_meta = sessions_df[sessions_df['session_id'] == selected_session].iloc[0]
        
        st.markdown("---")
        st.markdown(f"**Track:** {session_meta['track']}")
        st.markdown(f"**Car:** {session_meta['car']}")
        st.markdown(f"**Total Laps:** {session_meta['total_laps']}")
        st.markdown("---")
        
        # Lap dropdown using actual available laps from Athena
        with st.spinner("Fetching available laps..."):
            lap_options = fetch_available_laps(selected_session)
        
        if lap_options:
            selected_lap = st.selectbox("Select Lap to Analyze", lap_options)
        else:
            st.warning("No lap data available for this session.")
            selected_lap = None

    # ─── Main Content ───────────────────────────────────────────────────────
    
    if selected_lap is None:
        return

    # Load lap data
    with st.spinner(f"Querying Athena Data Lake for {selected_session} (Lap {selected_lap})..."):
        lap_df = fetch_lap_data(selected_session, selected_lap)

    if lap_df.empty:
        st.warning(f"No valid telemetry data found for Lap {selected_lap}.")
        return

    # Top level metrics
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        max_speed = lap_df['speed_kmh'].max()
        st.markdown('<div class="metric-label">Max Speed (km/h)</div>', unsafe_allow_html=True)
        st.markdown(f'<div class="metric-value">{max_speed:.1f}</div>', unsafe_allow_html=True)

    with col2:
        avg_speed = lap_df['speed_kmh'].mean()
        st.markdown('<div class="metric-label">Avg Speed (km/h)</div>', unsafe_allow_html=True)
        st.markdown(f'<div class="metric-value">{avg_speed:.1f}</div>', unsafe_allow_html=True)

    with col3:
        # Simple lap time calculation (assuming valid lap)
        lap_time_ms = int(lap_df['current_lap_time_ms'].max())
        seconds = (lap_time_ms / 1000) % 60
        minutes = int((lap_time_ms / (1000 * 60)) % 60)
        st.markdown('<div class="metric-label">Lap Time</div>', unsafe_allow_html=True)
        st.markdown(f'<div class="metric-value">{minutes}:{seconds:06.3f}</div>', unsafe_allow_html=True)
        
    with col4:
        fuel_used = lap_df['fuel_remaining'].max() - lap_df['fuel_remaining'].min()
        st.markdown('<div class="metric-label">Est. Fuel Used (L)</div>', unsafe_allow_html=True)
        st.markdown(f'<div class="metric-value">{fuel_used:.2f}</div>', unsafe_allow_html=True)

    st.markdown("---")
    
    # Charts tabs
    tab1, tab2 = st.tabs(["📊 Primary Telemetry", "🛞 Tyre Wear & Temps"])
    
    with tab1:
        st.plotly_chart(plot_telemetry_traces(lap_df), use_container_width=True)
        
    with tab2:
        st.plotly_chart(plot_tyre_temps(lap_df), use_container_width=True)


if __name__ == "__main__":
    main()
