"""Ship Analytics API - Real-time vessel tracking and anomaly detection."""

from datetime import UTC, datetime, timedelta
import os
from contextlib import asynccontextmanager
from typing import Optional

import psycopg
from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect
from psycopg import AsyncConnection
from psycopg.rows import dict_row

from services.api.phase2 import router as phase2_router
from services.websocket.handler import WebSocketHandler, get_websocket_handler
from ship_analytics.config import get_settings


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Initialize enhanced WebSocket handler on startup."""
    websocket_handler = await get_websocket_handler()
    app.state.websocket_handler = websocket_handler
    yield


app = FastAPI(title="Ship Analytics API", version="0.2.0", lifespan=lifespan)


async def connection() -> AsyncConnection[dict[str, object]]:
    return await AsyncConnection.connect(get_settings().database_url, row_factory=dict_row)


# WebSocket connection manager
class ConnectionManager:
    def __init__(self):
        self.active_connections: list[WebSocket] = []
        self.recent_updates: list[dict] = []

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.append(websocket)

    def disconnect(self, websocket: WebSocket):
        self.active_connections.remove(websocket)

    async def send_update(self, message: dict):
        self.recent_updates.append(message)
        if len(self.recent_updates) > 100:
            self.recent_updates = self.recent_updates[-100:]

        for connection in self.active_connections:
            try:
                await connection.send_json(message)
            except Exception:
                pass

    def get_recent_updates(self) -> list[dict]:
        return self.recent_updates.copy()


manager = ConnectionManager()


@app.get("/health")
async def health() -> dict[str, str]:
    """Health check endpoint."""
    try:
        async with await connection() as conn, conn.cursor() as cursor:
            await cursor.execute("SELECT 1")
            await cursor.fetchone()
    except psycopg.Error as exc:
        raise HTTPException(status_code=503, detail="database unavailable") from exc
    return {"status": "ok"}


@app.get("/metrics/summary")
async def summary(window_minutes: int = 5) -> dict[str, object]:
    """Return metrics for the latest available time window."""
    if window_minutes <= 0:
        raise HTTPException(status_code=422, detail="window_minutes must be positive")

    async with await connection() as conn, conn.cursor() as cursor:
        await cursor.execute(
            """
            WITH latest AS (
                SELECT MAX(event_time) AS latest_event_time
                FROM ais_event_clean
            ),
            reference AS (
                SELECT
                    latest_event_time,
                    CASE
                        WHEN latest_event_time IS NULL
                            OR latest_event_time >= now() - make_interval(mins => %s)
                        THEN now()
                        ELSE latest_event_time
                    END AS reference_time
                FROM latest
            )
            SELECT
              reference.reference_time,
              reference.latest_event_time,
              COUNT(*) FILTER (
                  WHERE event_time >= reference.reference_time - make_interval(mins => %s)
              ) AS event_count,
              COUNT(DISTINCT mmsi) FILTER (
                  WHERE event_time >= reference.reference_time - make_interval(mins => %s)
              ) AS active_vessels,
              AVG(sog_knots) FILTER (
                  WHERE event_time >= reference.reference_time - make_interval(mins => %s)
              ) AS average_speed_knots
            FROM ais_event_clean
            CROSS JOIN reference
            GROUP BY reference.reference_time, reference.latest_event_time
            """,
            (window_minutes, window_minutes, window_minutes, window_minutes),
        )
        metrics = await cursor.fetchone() or {}
        reference_time = metrics.get("reference_time")
        await cursor.execute(
            """
            SELECT COUNT(*) AS anomaly_count
            FROM anomaly
            WHERE event_time >= %s - make_interval(mins => %s)
            """,
            (reference_time or datetime.now(UTC), window_minutes),
        )
        anomalies = await cursor.fetchone() or {}
    return {
        "window_minutes": window_minutes,
        "reference_time": reference_time,
        "historical_reference": (
            reference_time is not None
            and reference_time < datetime.now(UTC) - timedelta(minutes=window_minutes)
        ),
        **metrics,
        **anomalies,
    }


@app.get("/vessels")
async def vessels(limit: int = 200) -> list[dict[str, object]]:
    """Return latest vessel states."""
    async with await connection() as conn, conn.cursor() as cursor:
        await cursor.execute(
            """
            SELECT mmsi, event_time, ST_Y(geom) AS latitude, ST_X(geom) AS longitude,
                   sog_knots, cog_degrees, vessel_type
            FROM vessel_latest ORDER BY event_time DESC LIMIT %s
            """,
            (limit,),
        )
        return list(await cursor.fetchall())


@app.get("/vessels/{mmsi}")
async def vessel(mmsi: int) -> dict[str, object]:
    """Return the latest state for a specific vessel."""
    async with await connection() as conn, conn.cursor() as cursor:
        await cursor.execute(
            """
            SELECT event_time, ST_Y(geom) AS latitude, ST_X(geom) AS longitude,
                   sog_knots, cog_degrees, vessel_type, heading_degrees, nav_status
            FROM vessel_latest WHERE mmsi = %s ORDER BY event_time DESC LIMIT 1
            """,
            (mmsi,),
        )
        return await cursor.fetchone() or {}


@app.get("/vessels/{mmsi}/track")
async def vessel_track(mmsi: int, start: datetime, end: datetime) -> dict[str, object]:
    """Return track history for a specific vessel."""
    if start >= end:
        raise HTTPException(status_code=422, detail="start must precede end")
    async with await connection() as conn, conn.cursor() as cursor:
        await cursor.execute(
            """
            SELECT event_time, ST_X(geom) AS longitude, ST_Y(geom) AS latitude, sog_knots, cog_degrees
            FROM vessel_track_point
            WHERE mmsi = %s AND event_time >= %s AND event_time <= %s
            ORDER BY event_time
            """,
            (mmsi, start, end),
        )
        points = list(await cursor.fetchall())
    return {
        "type": "FeatureCollection",
        "features": [
            {
                "type": "Feature",
                "geometry": {"type": "Point", "coordinates": [p["longitude"], p["latitude"]]},
                "properties": {k: v for k, v in p.items() if k not in {"longitude", "latitude"}},
            }
            for p in points
        ],
    }


@app.get("/anomalies")
async def anomalies(
    from_time: datetime,
    to_time: datetime,
    anomaly_type: str | None = None,
    bbox: tuple[float, float, float, float] | None = None,
) -> list[dict[str, object]]:
    """Return anomaly events within a time range."""
    async with await connection() as conn, conn.cursor() as cursor:
        query = """
        SELECT mmsi, event_time, anomaly_type, severity, explanation,
               ST_Y(geom) AS latitude, ST_X(geom) AS longitude
        FROM anomaly
        WHERE event_time >= %s AND event_time <= %s
        """
        params = [from_time, to_time]

        if anomaly_type:
            query += " AND anomaly_type = %s"
            params.append(anomaly_type)

        if bbox:
            min_lat, min_lon, max_lat, max_lon = bbox
            query += " AND ST_Within(geom, ST_MakeBox2d(ST_Point(%s, %s), ST_Point(%s, %s)))"
            params.extend([min_lon, min_lat, max_lon, max_lat])

        query += " ORDER BY event_time DESC"

        await cursor.execute(query, params)
        return list(await cursor.fetchall())


@app.get("/window-metrics")
async def window_metrics(
    window_minutes: int = 60,
    metric_names: str | None = None,
    dimensions: str | None = None,
) -> list[dict[str, object]]:
    """Return window metrics for time periods."""
    async with await connection() as conn, conn.cursor() as cursor:
        await cursor.execute("SELECT event_time FROM ais_event_clean ORDER BY event_time LIMIT 100")
        events = [row["event_time"] for row in await cursor.fetchall()]

        if not events:
            return []

        min_time = min(events)
        max_time = max(events)

        window_start = min_time.replace(
            minute=(min_time.minute // window_minutes) * window_minutes,
            second=0,
            microsecond=0,
        )

        windows = []
        current_time = window_start
        while current_time < max_time:
            window_end = current_time + timedelta(minutes=window_minutes)

            await cursor.execute(
                """
                SELECT metric_name, dimensions_json, value
                FROM window_metric
                WHERE window_start >= %s AND window_end <= %s
                ORDER BY window_start, metric_name
                """,
                (current_time, window_end),
            )
            metrics = await cursor.fetchall()

            if metrics:
                windows.extend(metrics)

            current_time = window_end

    return windows


@app.websocket("/ws/updates")
async def websocket_updates(websocket: WebSocket, token: Optional[str] = None):
    """Enhanced WebSocket endpoint for real-time updates with authentication."""
    # Get the enhanced WebSocket handler
    websocket_handler = app.state.websocket_handler
    await websocket_handler.handle_websocket(websocket, token)
    

@app.websocket("/ws/updates-legacy")
async def websocket_updates_legacy(websocket: WebSocket):
    """Legacy WebSocket endpoint for backward compatibility."""
    await manager.connect(websocket)
    
    try:
        while True:
            data = await websocket.receive_json()
            # Process incoming messages if needed
            await manager.send_update({
                "type": "client_message",
                "data": data,
                "timestamp": datetime.utcnow().isoformat(),
            })
    except WebSocketDisconnect:
        manager.disconnect(websocket)

app.include_router(phase2_router)
