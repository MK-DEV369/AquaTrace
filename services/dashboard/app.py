"""Interactive Streamlit dashboard for AquaTrace vessel operations."""

import os
from datetime import UTC, datetime, timedelta

import pandas as pd
import pydeck as pdk
import requests
import streamlit as st

API_BASE_URL = os.getenv("API_BASE_URL", "http://localhost:8000")
API_AUTH_TOKEN = os.getenv("API_AUTH_TOKEN")
SHIP_ICON = (
    "data:image/svg+xml;charset=utf-8,"
    "%3Csvg xmlns='http://www.w3.org/2000/svg' width='64' height='64' "
    "viewBox='0 0 64 64'%3E%3Cpath fill='%2349d6ff' stroke='%23ffffff' "
    "stroke-width='3' d='M32 4 56 56 32 47 8 56Z'/%3E%3C/svg%3E"
)

st.set_page_config(
    page_title="AquaTrace | Maritime Intelligence",
    page_icon="⚓",
    layout="wide",
    initial_sidebar_state="expanded",
)
st.markdown(
    """
    <style>
    .stApp { background: #07111f; }
    [data-testid="stHeader"] { background: rgba(7, 17, 31, .88); }
    [data-testid="stSidebar"] { background: #0b1a2c; border-right: 1px solid #1e3857; }
    .hero { padding: 1.7rem 2rem; border: 1px solid #24527a; border-radius: 22px;
            background: radial-gradient(circle at 85% 20%, #164d72 0, transparent 38%),
                        linear-gradient(120deg, #102d4a, #101f35 55%, #091625);
            margin-bottom: 1rem; box-shadow: 0 16px 45px rgba(0,0,0,.2); }
    .eyebrow { color: #58d6ff; font-size: .75rem; font-weight: 800; letter-spacing: .18em; }
    .hero h1 { color: #f4f8ff; margin: .25rem 0; font-size: 2.25rem; }
    .hero p { color: #a4bad2; margin: 0; }
    .status { color: #74e6a3; font-weight: 700; }
    .pulse { display: inline-block; width: 8px; height: 8px; border-radius: 50%;
             background: #74e6a3; box-shadow: 0 0 0 0 rgba(116,230,163,.7);
             animation: pulse 1.8s infinite; margin-right: .35rem; }
    @keyframes pulse { 70% { box-shadow: 0 0 0 9px rgba(116,230,163,0); } 100% { box-shadow: 0 0 0 0 rgba(116,230,163,0); } }
    div[data-testid="stMetric"] { background: #0e2035; border: 1px solid #1e3d5d;
        padding: 1rem; border-radius: 16px; }
    div[data-testid="stMetricLabel"] { color: #8fa9c5; }
    div[data-testid="stMetricValue"] { color: #f4f8ff; }
    .map-note { color: #8fa9c5; font-size: .82rem; padding: .5rem 0; }
    </style>
    """,
    unsafe_allow_html=True,
)


@st.cache_data(ttl=8, show_spinner=False)
def fetch(path: str, params: dict[str, object] | None = None) -> object:
    headers = {"Authorization": f"Bearer {API_AUTH_TOKEN}"} if API_AUTH_TOKEN else {}
    response = requests.get(f"{API_BASE_URL}{path}", params=params, headers=headers, timeout=8)
    response.raise_for_status()
    return response.json()


def isoformat(value: datetime) -> str:
    return value.astimezone(UTC).isoformat()


def build_map(
    vessels: pd.DataFrame,
    track: pd.DataFrame,
    anomalies: pd.DataFrame,
    *,
    show_labels: bool,
    show_route: bool,
    show_anomalies: bool,
) -> pdk.Deck:
    layers: list[pdk.Layer] = []
    vessel_data = vessels.copy()
    vessel_data["icon_data"] = [
        {"url": SHIP_ICON, "width": 64, "height": 64, "anchorY": 32}
        for _ in range(len(vessel_data))
    ]
    vessel_data["label"] = vessel_data["mmsi"].astype(str)
    vessel_data["speed_color"] = vessel_data["sog_knots"].fillna(0).apply(
        lambda speed: [255, 104, 120, 230] if speed >= 18 else [73, 214, 255, 230]
    )
    layers.append(
        pdk.Layer(
            "IconLayer",
            data=vessel_data,
            get_icon="icon_data",
            get_position="[longitude, latitude]",
            get_size=4,
            size_scale=8,
            pickable=True,
            auto_highlight=True,
            tooltip=True,
        )
    )
    layers.append(
        pdk.Layer(
            "ScatterplotLayer",
            data=vessel_data,
            get_position="[longitude, latitude]",
            get_radius=850,
            get_fill_color="speed_color",
            radius_min_pixels=5,
            radius_max_pixels=18,
            opacity=0.25,
            stroked=True,
            get_line_color=[73, 214, 255, 180],
            pickable=True,
        )
    )
    if show_labels:
        layers.append(
            pdk.Layer(
                "TextLayer",
                data=vessel_data,
                get_position="[longitude, latitude]",
                get_text="label",
                get_size=11,
                get_color=[224, 242, 255, 230],
                get_pixel_offset=[0, 28],
                billboard=True,
            )
        )
    if show_route and not track.empty:
        route_points = track[["longitude", "latitude"]].to_dict("records")
        route_path = [{"path": [[p["longitude"], p["latitude"]] for p in route_points]}]
        layers.extend(
            [
                pdk.Layer(
                    "PathLayer",
                    data=route_path,
                    get_path="path",
                    get_color=[73, 214, 255, 185],
                    width_min_pixels=2,
                    get_width=3,
                    opacity=0.65,
                ),
                pdk.Layer(
                    "ScatterplotLayer",
                    data=track,
                    get_position="[longitude, latitude]",
                    get_radius=120,
                    get_fill_color=[164, 238, 255, 220],
                    radius_min_pixels=2,
                    radius_max_pixels=5,
                ),
            ]
        )
    if show_anomalies and not anomalies.empty:
        layers.append(
            pdk.Layer(
                "ScatterplotLayer",
                data=anomalies,
                get_position="[longitude, latitude]",
                get_radius=1300,
                get_fill_color=[255, 92, 116, 220],
                get_line_color=[255, 220, 225, 240],
                line_width_min_pixels=2,
                stroked=True,
                radius_min_pixels=7,
                radius_max_pixels=20,
                pickable=True,
            )
        )
    center_lat = float(vessels["latitude"].mean()) if not vessels.empty else 20
    center_lon = float(vessels["longitude"].mean()) if not vessels.empty else 0
    return pdk.Deck(
        layers=layers,
        initial_view_state=pdk.ViewState(
            latitude=center_lat,
            longitude=center_lon,
            zoom=3.2 if len(vessels) > 1 else 6,
            pitch=35,
        ),
        map_style="https://basemaps.cartocdn.com/gl/dark-matter-gl-style/style.json",
        tooltip={
            "html": "<b>MMSI:</b> {mmsi}<br/><b>Speed:</b> {sog_knots} kn<br/>"
            "<b>Course:</b> {cog_degrees}°",
            "style": {"backgroundColor": "#10263d", "color": "#f4f8ff"},
        },
        parameters={"cursor": "grab"},
    )


st.sidebar.markdown("## ⚓ AquaTrace")
st.sidebar.caption("Real-time vessel intelligence")
window_minutes = st.sidebar.select_slider(
    "Analytics window",
    options=[5, 15, 30, 60, 180],
    value=15,
    format_func=lambda value: f"{value} minutes",
)
trail_minutes = st.sidebar.slider("Route trail", 15, 360, 90, step=15, format="%d minutes")
show_route = st.sidebar.checkbox("Show dotted route", True)
show_labels = st.sidebar.checkbox("Show MMSI labels", True)
show_anomalies = st.sidebar.checkbox("Show anomaly beacons", True)
if st.sidebar.button("↻ Refresh data", use_container_width=True):
    fetch.clear()
    st.rerun()
st.sidebar.divider()
st.sidebar.caption("Data source")
st.sidebar.code(API_BASE_URL, language="text")
st.sidebar.caption("Map controls: scroll to zoom • drag to pan • hover a vessel")

try:
    fetch("/health")
    metrics = fetch("/metrics/summary", {"window_minutes": window_minutes})
    vessel_rows = fetch("/vessels", {"limit": 500})
except requests.RequestException as exc:
    st.error(f"The API is not ready: {exc}")
    st.stop()

vessels = pd.DataFrame(vessel_rows)
if not vessels.empty:
    vessels["mmsi"] = vessels["mmsi"].astype(int)
    vessels["sog_knots"] = pd.to_numeric(vessels["sog_knots"], errors="coerce").fillna(0)
    vessels["cog_degrees"] = pd.to_numeric(vessels["cog_degrees"], errors="coerce").fillna(0)
reference_time = metrics.get("reference_time")
reference = (
    datetime.fromisoformat(reference_time.replace("Z", "+00:00")).astimezone(UTC)
    if reference_time
    else datetime.now(UTC)
)
reference_label = reference.strftime("%d %b %Y, %H:%M UTC")
mode_label = "Historical replay window" if metrics.get("historical_reference") else "Live window"

st.markdown(
    """
    <div class="hero">
      <div class="eyebrow">MARITIME OPERATIONS CENTER</div>
      <h1>Fleet pulse, in motion</h1>
      <p><span class="pulse"></span>Vessel movement, route activity, and anomaly signals
      <span class="status">● API online</span></p>
    </div>
    """,
    unsafe_allow_html=True,
)
st.caption(f"{mode_label} • reference time: {reference_label} • trail: {trail_minutes} min")

columns = st.columns(4)
columns[0].metric(f"Events · {window_minutes} min", metrics.get("event_count", 0))
columns[1].metric("Active vessels", metrics.get("active_vessels", 0))
columns[2].metric("Mean speed", f"{metrics.get('average_speed_knots') or 0:.1f} kn")
columns[3].metric("Anomalies", metrics.get("anomaly_count", 0))

if vessels.empty:
    st.info("No processed vessels yet. Start the Spark processor, then run the replay producer.")
    st.stop()

vessel_options = ["All vessels"] + [str(mmsi) for mmsi in vessels["mmsi"].unique()]
selected = st.selectbox("Focus vessel", vessel_options, index=0)
selected_mmsi = None if selected == "All vessels" else int(selected)
map_vessels = vessels if selected_mmsi is None else vessels[vessels["mmsi"] == selected_mmsi]
replay_percent = 100
if selected_mmsi is not None and show_route:
    replay_percent = st.slider(
        "Route playback position",
        min_value=10,
        max_value=100,
        value=100,
        step=10,
        format="%d%% of trail",
        help="Scrub the selected vessel's dotted route to inspect its movement over time.",
    )

track = pd.DataFrame()
if selected_mmsi is not None and show_route:
    start = reference - timedelta(minutes=trail_minutes)
    try:
        track_payload = fetch(
            f"/vessels/{selected_mmsi}/track",
            {"start": isoformat(start), "end": isoformat(reference)},
        )
        track = pd.DataFrame(
            [
                {
                    "event_time": feature["properties"].get("event_time"),
                    "longitude": feature["geometry"]["coordinates"][0],
                    "latitude": feature["geometry"]["coordinates"][1],
                    "sog_knots": feature["properties"].get("sog_knots"),
                }
                for feature in track_payload.get("features", [])
            ]
        )
        if not track.empty and replay_percent < 100:
            track = track.iloc[: max(1, round(len(track) * replay_percent / 100))]
    except requests.RequestException as exc:
        st.warning(f"Could not load route history: {exc}")

anomalies = pd.DataFrame()
if show_anomalies:
    try:
        anomalies = pd.DataFrame(
            fetch(
                "/anomalies",
                {
                    "from_time": isoformat(reference - timedelta(minutes=window_minutes)),
                    "to_time": isoformat(reference),
                },
            )
        )
        if selected_mmsi is not None and not anomalies.empty:
            anomalies = anomalies[anomalies["mmsi"].astype(int) == selected_mmsi]
    except requests.RequestException as exc:
        st.warning(f"Could not load anomaly beacons: {exc}")

map_col, insight_col = st.columns([1.7, 1])
with map_col:
    st.subheader("Live fleet map")
    if selected_mmsi is not None:
        st.markdown(
            '<div class="map-note">Cyan dots form the selected vessel\'s dotted trail. '
            "Red beacons mark anomaly events.</div>",
            unsafe_allow_html=True,
        )
    st.pydeck_chart(
        build_map(
            map_vessels,
            track,
            anomalies,
            show_labels=show_labels,
            show_route=show_route and selected_mmsi is not None,
            show_anomalies=show_anomalies,
        ),
        use_container_width=True,
    )
with insight_col:
    st.subheader("Fleet activity")
    speed_data = vessels.set_index(vessels["mmsi"].astype(str))[["sog_knots"]]
    st.bar_chart(speed_data, color="#49d6ff", height=245)
    if selected_mmsi is not None:
        selected_row = vessels[vessels["mmsi"] == selected_mmsi].iloc[0]
        st.info(
            f"MMSI {selected_mmsi} • {selected_row.get('vessel_type') or 'Unknown type'}\n\n"
            f"Speed **{selected_row['sog_knots']:.1f} kn** • course **{selected_row['cog_degrees']:.0f}°**"
        )
        if not track.empty:
            st.metric("Trail points", len(track))

tab_register, tab_anomalies, tab_route = st.tabs(["Vessel register", "Anomaly feed", "Route telemetry"])
with tab_register:
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
with tab_anomalies:
    if anomalies.empty:
        st.success("No anomaly events in the selected window.")
    else:
        st.dataframe(
            anomalies,
            use_container_width=True,
            hide_index=True,
            column_config={
                "severity": st.column_config.TextColumn("Severity"),
                "explanation": st.column_config.TextColumn("Explanation", width="large"),
            },
        )
with tab_route:
    if selected_mmsi is None:
        st.info("Select a vessel above to inspect its route telemetry.")
    elif track.empty:
        st.info("No track points were returned for this trail window.")
    else:
        telemetry = track.copy()
        telemetry["event_time"] = pd.to_datetime(telemetry["event_time"]).dt.strftime("%H:%M:%S")
        st.line_chart(telemetry.set_index("event_time")[["sog_knots"]], color="#74e6a3")
        st.dataframe(telemetry, use_container_width=True, hide_index=True)
