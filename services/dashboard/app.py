import os
from datetime import UTC, datetime

import pandas as pd
import requests
import streamlit as st

API_BASE_URL = os.getenv("API_BASE_URL", "http://localhost:8000")

st.set_page_config(
    page_title="AquaTrace | Maritime Intelligence",
    page_icon="⚓",
    layout="wide",
    initial_sidebar_state="expanded",
)
st.markdown(
    """
    <style>
    .stApp { background: #08111f; }
    [data-testid="stHeader"] { background: rgba(8, 17, 31, .92); }
    [data-testid="stSidebar"] { background: #0d1b2e; border-right: 1px solid #203553; }
    .hero { padding: 1.5rem 1.8rem; border: 1px solid #23466a; border-radius: 18px;
            background: linear-gradient(120deg, #102c4b, #12213a 55%, #0b1728);
            margin-bottom: 1rem; }
    .eyebrow { color: #58d6ff; font-size: .78rem; font-weight: 700; letter-spacing: .16em; }
    .hero h1 { color: #f4f8ff; margin: .25rem 0; font-size: 2.1rem; }
    .hero p { color: #9eb6d0; margin: 0; }
    .status { color: #74e6a3; font-weight: 700; }
    div[data-testid="stMetric"] { background: #101f34; border: 1px solid #203b5b;
        padding: 1rem; border-radius: 14px; }
    div[data-testid="stMetricLabel"] { color: #8fa9c5; }
    div[data-testid="stMetricValue"] { color: #f4f8ff; }
    </style>
    """,
    unsafe_allow_html=True,
)


def fetch(path: str) -> object:
    response = requests.get(f"{API_BASE_URL}{path}", timeout=5)
    response.raise_for_status()
    return response.json()


st.sidebar.markdown("## ⚓ AquaTrace")
st.sidebar.caption("Real-time vessel intelligence")
window_minutes = st.sidebar.select_slider(
    "Analytics window",
    options=[5, 15, 30, 60],
    value=5,
    format_func=lambda value: f"{value} minutes",
)
if st.sidebar.button("↻ Refresh data", use_container_width=True):
    st.cache_data.clear()
    st.rerun()
st.sidebar.divider()
st.sidebar.caption("Data source")
st.sidebar.code(API_BASE_URL, language="text")

try:
    fetch("/health")
    metrics = fetch(f"/metrics/summary?window_minutes={window_minutes}")
    vessel_rows = fetch("/vessels?limit=500")
except requests.RequestException as exc:
    st.error(f"The API is not ready: {exc}")
    st.stop()

reference_time = metrics.get("reference_time")
reference_label = "unknown"
if reference_time:
    reference_label = datetime.fromisoformat(reference_time.replace("Z", "+00:00")).astimezone(
        UTC
    ).strftime("%d %b %Y, %H:%M UTC")
mode_label = "Historical replay window" if metrics.get("historical_reference") else "Live window"

st.markdown(
    """
    <div class="hero">
      <div class="eyebrow">MARITIME OPERATIONS CENTER</div>
      <h1>Fleet pulse, at a glance</h1>
      <p>Vessel movement, route activity, and anomaly signals in one view.
      <span class="status">● API online</span></p>
    </div>
    """,
    unsafe_allow_html=True,
)
st.caption(f"{mode_label} • reference time: {reference_label}")

columns = st.columns(4)
columns[0].metric(f"Events · {window_minutes} min", metrics.get("event_count", 0))
columns[1].metric("Active vessels", metrics.get("active_vessels", 0))
columns[2].metric(
    "Mean speed",
    f"{metrics.get('average_speed_knots') or 0:.1f} kn",
)
columns[3].metric("Anomalies", metrics.get("anomaly_count", 0))

vessels = pd.DataFrame(vessel_rows)
if vessels.empty:
    st.info("No processed vessels yet. Start the Spark processor, then run the replay producer.")
else:
    map_data = vessels.rename(columns={"latitude": "lat", "longitude": "lon"})
    map_col, chart_col = st.columns([1.65, 1])
    with map_col:
        st.subheader("Fleet map")
        st.map(map_data, latitude="lat", longitude="lon", size=90, color="#49d6ff")
    with chart_col:
        st.subheader("Speed by vessel")
        speed_data = vessels.set_index(vessels["mmsi"].astype(str))[["sog_knots"]]
        st.bar_chart(speed_data, color="#49d6ff", height=280)

    st.subheader("Vessel register")
    vessel_filter = st.text_input("Filter by MMSI or vessel type", placeholder="e.g. Cargo")
    display = vessels.copy()
    if vessel_filter:
        needle = vessel_filter.lower()
        display = display[
            display.astype(str).apply(lambda column: column.str.lower().str.contains(needle)).any(axis=1)
        ]
    display["event_time"] = pd.to_datetime(display["event_time"]).dt.strftime("%d %b %H:%M UTC")
    st.dataframe(
        display[
            ["mmsi", "event_time", "latitude", "longitude", "sog_knots", "cog_degrees", "vessel_type"]
        ],
        use_container_width=True,
        hide_index=True,
        column_config={
            "mmsi": st.column_config.NumberColumn("MMSI", format="%d"),
            "latitude": st.column_config.NumberColumn("Latitude", format="%.4f"),
            "longitude": st.column_config.NumberColumn("Longitude", format="%.4f"),
            "sog_knots": st.column_config.NumberColumn("Speed (kn)", format="%.1f"),
            "cog_degrees": st.column_config.NumberColumn("Course", format="%.0f°"),
        },
    )
