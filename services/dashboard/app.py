import os

import pandas as pd
import requests
import streamlit as st

API_BASE_URL = os.getenv("API_BASE_URL", "http://localhost:8000")

st.set_page_config(page_title="Ship Analytics", layout="wide")
st.title("Ship Analytics")
st.caption("Development dashboard - replay data is the primary demonstration source.")

try:
    health = requests.get(f"{API_BASE_URL}/health", timeout=3)
    health.raise_for_status()
    metrics = requests.get(f"{API_BASE_URL}/metrics/summary", timeout=3).json()
    vessel_rows = requests.get(f"{API_BASE_URL}/vessels?limit=500", timeout=3).json()
except requests.RequestException as exc:
    st.error(f"The API is not ready: {exc}")
    st.stop()

columns = st.columns(4)
columns[0].metric("Events (5 min)", metrics.get("event_count", 0))
columns[1].metric("Active vessels", metrics.get("active_vessels", 0))
columns[2].metric("Mean speed (kn)", f"{metrics.get('average_speed_knots') or 0:.1f}")
columns[3].metric("Anomalies", metrics.get("anomaly_count", 0))

vessels = pd.DataFrame(vessel_rows)
if vessels.empty:
    st.info("No processed vessels yet. Start the Spark processor, then run the replay producer.")
else:
    st.map(vessels.rename(columns={"latitude": "lat", "longitude": "lon"}), latitude="lat", longitude="lon")
    st.dataframe(vessels, use_container_width=True, hide_index=True)

