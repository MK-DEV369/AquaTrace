"""Ship Analytics API - Real-time vessel tracking and anomaly detection."""

import secrets
import uuid
from contextlib import asynccontextmanager
from datetime import UTC, datetime, timedelta
from math import asin, atan2, cos, degrees, radians, sin

import psycopg
from fastapi import FastAPI, HTTPException, Request, Response, WebSocket
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from fastapi.staticfiles import StaticFiles
from psycopg import AsyncConnection

from services.api.phase2 import router as phase2_router
from services.websocket.handler import get_websocket_handler
from ship_analytics.agents import get_orchestrator, get_registry
from ship_analytics.config import get_settings
from ship_analytics.logging import (
    configure_logging,
    get_correlation_id,
    get_logger,
    set_correlation_id,
)
from ship_analytics.metrics import render as render_metrics


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Initialize enhanced WebSocket handler on startup."""
    configure_logging(get_settings().log_level)
    websocket_handler = await get_websocket_handler()
    app.state.websocket_handler = websocket_handler
    app.state.registry = get_registry()
    app.state.orchestrator = get_orchestrator(app.state.registry)
    await app.state.registry.start_all()
    yield
    await app.state.registry.stop_all()


app = FastAPI(title="Ship Analytics API", version="0.2.0", lifespan=lifespan)
app.mount("/force-graph", StaticFiles(directory="services/force_graph", html=True), name="force-graph")
app.include_router(phase2_router)
logger = get_logger("api")


@app.get("/favicon.ico", include_in_schema=False)
async def favicon() -> Response:
    """Prevent browsers from receiving an authentication error for a missing favicon."""
    return Response(status_code=204)


@app.middleware("http")
async def request_context_and_auth(request: Request, call_next):
    """Attach correlation IDs and enforce API access before dispatching a request."""
    cid = request.headers.get("x-correlation-id") or uuid.uuid4().hex
    set_correlation_id(cid)
    public_paths = {
        "/health", "/docs", "/openapi.json", "/redoc", "/metrics", "/agents", "/agents/trace", "/favicon.ico",
    }
    local_graph_data = (
        request.url.path == "/force-graph-data"
        and get_settings().environment.lower() in {"local", "development", "test"}
    )
    if request.url.path in public_paths or request.url.path.startswith("/force-graph/") or local_graph_data:
        response = await call_next(request)
        response.headers["X-Correlation-ID"] = cid
        return response

    configured_token = get_settings().api_token
    supplied_token = request.headers.get("authorization", "")
    if not configured_token or not supplied_token.startswith("Bearer "):
        response = JSONResponse(
            status_code=401,
            content={"error": "authentication required", "correlation_id": cid},
        )
        response.headers["X-Correlation-ID"] = cid
        return response

    token = supplied_token[7:].strip()
    if not token or not secrets.compare_digest(token, configured_token):
        response = JSONResponse(
            status_code=401,
            content={"error": "invalid authentication token", "correlation_id": cid},
        )
        response.headers["X-Correlation-ID"] = cid
        return response

    response = await call_next(request)
    response.headers["X-Correlation-ID"] = cid
    return response


@app.exception_handler(Exception)
async def generic_exception_handler(request: Request, exc: Exception):
    """Return a generic error envelope; never leak raw exception details."""
    cid = get_correlation_id()
    logger.exception("unhandled api error path=%s", request.url.path)
    return JSONResponse(
        status_code=500,
        content={"error": "internal server error", "correlation_id": cid, "path": str(request.url.path)},
    )


@app.exception_handler(HTTPException)
async def http_exception_handler(request: Request, exc: HTTPException):
    """Keep expected HTTP failures useful without exposing implementation details."""
    cid = get_correlation_id()
    message = "service unavailable" if exc.status_code >= 500 else "request rejected"
    logger.warning("api request rejected status=%d path=%s", exc.status_code, request.url.path)
    return JSONResponse(
        status_code=exc.status_code,
        content={"error": message, "correlation_id": cid, "path": str(request.url.path)},
    )


@app.exception_handler(RequestValidationError)
async def validation_exception_handler(request: Request, exc: RequestValidationError):
    """Avoid returning unbounded framework validation detail to clients."""
    cid = get_correlation_id()
    logger.info("invalid api request path=%s errors=%d", request.url.path, len(exc.errors()))
    return JSONResponse(
        status_code=422,
        content={"error": "invalid request", "correlation_id": cid, "path": str(request.url.path)},
    )


async def connection() -> AsyncConnection[dict[str, object]]:
    from services.api.db import connection as db_connection

    return await db_connection()


def _mount_agent_router() -> None:
    from services.api.agent_router import router as agent_router

    app.include_router(agent_router)


_mount_agent_router()


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


@app.get("/force-graph-data", include_in_schema=False)
async def force_graph_data(limit: int = 400) -> list[dict[str, object]]:
    """Serve graph data without exposing an API token in the browser during local demos."""
    return await vessels(limit=min(limit, 1_000))


@app.get("/vessels/{mmsi}")
async def vessel(mmsi: int) -> dict[str, object]:
    """Return the latest state for a specific vessel."""
    async with await connection() as conn, conn.cursor() as cursor:
        await cursor.execute(
            """
            SELECT v.event_time, ST_Y(v.geom) AS latitude, ST_X(v.geom) AS longitude,
                   v.sog_knots, v.cog_degrees, v.vessel_type,
                   e.heading_degrees, e.nav_status, e.source
            FROM vessel_latest v
            LEFT JOIN LATERAL (
                SELECT heading_degrees, nav_status, source
                FROM ais_event_clean
                WHERE mmsi = v.mmsi
                ORDER BY event_time DESC
                LIMIT 1
            ) e ON TRUE
            WHERE v.mmsi = %s
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
        if not points:
            # Fallback: derive a track from raw clean AIS events when the
            # materialised track table has not been populated yet.
            await cursor.execute(
                """
                SELECT event_time, ST_X(geom) AS longitude, ST_Y(geom) AS latitude, sog_knots, cog_degrees
                FROM ais_event_clean
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


@app.get("/vessels/{mmsi}/route-forecast")
async def vessel_route_forecast(mmsi: int, horizon_minutes: int = 30, steps: int = 6) -> dict[str, object]:
    """Project a constant-course, constant-speed route from the latest AIS position."""
    horizon_minutes = min(max(horizon_minutes, 5), 240)
    steps = min(max(steps, 2), 24)
    async with await connection() as conn, conn.cursor() as cursor:
        await cursor.execute(
            """
            SELECT event_time, ST_Y(geom) AS latitude, ST_X(geom) AS longitude,
                   COALESCE(sog_knots, 0) AS sog_knots, COALESCE(cog_degrees, 0) AS cog_degrees
            FROM vessel_latest WHERE mmsi = %s
            """,
            (mmsi,),
        )
        latest = await cursor.fetchone()
    if not latest:
        raise HTTPException(status_code=404, detail="vessel not found")

    latitude, longitude = float(latest["latitude"]), float(latest["longitude"])
    speed, course = float(latest["sog_knots"]), float(latest["cog_degrees"])
    positions = []
    for step in range(1, steps + 1):
        minutes = horizon_minutes * step / steps
        angular_distance = (speed * (minutes / 60.0)) / 3440.065
        bearing = radians(course)
        lat1, lon1 = radians(latitude), radians(longitude)
        lat2 = asin(sin(lat1) * cos(angular_distance) + cos(lat1) * sin(angular_distance) * cos(bearing))
        lon2 = lon1 + atan2(
            sin(bearing) * sin(angular_distance) * cos(lat1),
            cos(angular_distance) - sin(lat1) * sin(lat2),
        )
        positions.append(
            {
                "minutes_ahead": round(minutes),
                "latitude": degrees(lat2),
                "longitude": (degrees(lon2) + 540) % 360 - 180,
                "sog_knots": speed,
                "cog_degrees": course,
            }
        )
    return {
        "mmsi": mmsi,
        "method": "constant_course_speed",
        "observed_at": latest["event_time"],
        "positions": positions,
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
async def websocket_updates(websocket: WebSocket, token: str | None = None):
    """Enhanced WebSocket endpoint for real-time updates with authentication."""
    # Get the enhanced WebSocket handler
    websocket_handler = app.state.websocket_handler
    await websocket_handler.handle_websocket(websocket, token)
    

@app.get("/metrics")
async def metrics() -> str:
    """Expose in-process metrics in Prometheus text format."""
    from fastapi.responses import PlainTextResponse

    return PlainTextResponse(render_metrics(), media_type="text/plain")


@app.get("/agents")
async def agents() -> dict[str, object]:
    """List registered agents and their capabilities."""
    registry = app.state.registry
    orchestrator = app.state.orchestrator
    return {
        "registry": registry.snapshot(),
        "health": await orchestrator.health(),
        "trace": orchestrator.trace_history(50),
    }


@app.get("/agents/trace")
async def agents_trace(limit: int = 100) -> list[dict[str, object]]:
    """Return the recent orchestrator trace (live processing view)."""
    return app.state.orchestrator.trace_history(limit)


@app.post("/agents/query")
async def agents_query(payload: dict[str, object]) -> dict[str, object]:
    """Answer a natural-language query through the registered query agent."""
    query = str(payload.get("query", "")).strip()
    if not query:
        raise HTTPException(status_code=422, detail="query is required")
    return await app.state.orchestrator.run(
        {"query": query, "context": payload.get("context")},
        requirements=["natural_language_query"],
        correlation_id=get_correlation_id(),
    )


@app.post("/agents/tasks")
async def agents_tasks(payload: dict[str, object]) -> dict[str, object]:
    """Execute an explicit dependency-aware multi-agent task graph."""
    from ship_analytics.agents import AgentTask

    raw_tasks = payload.get("tasks")
    if not isinstance(raw_tasks, list) or not raw_tasks:
        raise HTTPException(status_code=422, detail="tasks must be a non-empty list")
    tasks = [
        AgentTask(
            task_id=str(item["task_id"]),
            requirement=str(item["requirement"]),
            payload=dict(item.get("payload", {})),
            depends_on=[str(value) for value in item.get("depends_on", [])],
        )
        for item in raw_tasks
        if isinstance(item, dict) and "task_id" in item and "requirement" in item
    ]
    if len(tasks) != len(raw_tasks):
        raise HTTPException(status_code=422, detail="each task needs task_id and requirement")
    return await app.state.orchestrator.run_task_graph(tasks, get_correlation_id())


@app.get("/kafka/offsets")
async def kafka_offsets() -> dict[str, object]:
    """Report consumed/end offsets per partition for ais.raw."""
    from confluent_kafka import Consumer, TopicPartition

    settings = get_settings()
    kafka_config = {
        "bootstrap.servers": settings.kafka_brokers,
        "group.id": "offset-diagnostic",
        "enable.auto.commit": False,
        "security.protocol": settings.kafka_security_protocol,
    }
    if settings.kafka_security_protocol in {"SASL_PLAINTEXT", "SASL_SSL"}:
        kafka_config.update(
            {
                "sasl.mechanism": settings.kafka_sasl_mechanism,
                "sasl.username": settings.kafka_sasl_username,
                "sasl.password": settings.kafka_sasl_password,
            }
        )
    if settings.kafka_security_protocol in {"SSL", "SASL_SSL"}:
        kafka_config["ssl.ca.location"] = settings.kafka_ssl_ca_location
    consumer = Consumer(
        kafka_config
    )
    try:
        metadata = consumer.list_topics("ais.raw", timeout=5)
        topic = metadata.topics.get("ais.raw")
        if topic is None:
            return {"topic": "ais.raw", "partitions": [], "error": "topic not found"}
        partitions = []
        for pid in topic.partitions:
            tp = TopicPartition("ais.raw", pid)
            low, high = consumer.get_watermark_offsets(tp, timeout=5)
            committed = consumer.committed([tp], timeout=5)[0].offset
            partitions.append(
                {
                    "partition": pid,
                    "low_watermark": low,
                    "high_watermark": high,
                    "committed_offset": committed,
                }
            )
        return {"topic": "ais.raw", "partitions": partitions}
    finally:
        consumer.close()
