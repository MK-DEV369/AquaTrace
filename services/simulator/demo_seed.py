"""Synthetic fleet + anomaly simulator for demos and development.

Generates moving vessels (some anomalous) and persists them directly to
PostgreSQL/PostGIS so the dashboard, anomaly feed and investigation tools
have data even without a live Kafka/Spark pipeline.
"""
from __future__ import annotations

import math
import random
from datetime import UTC, datetime, timedelta
from typing import Any

import psycopg

from ship_analytics.config import get_settings


def _connect():
    return psycopg.connect(get_settings().database_url)


def _point(lat: float, lon: float) -> str:
    return f"ST_SetSRID(ST_MakePoint({lon}, {lat}), 4326)"


def seed_demo_fleet(
    vessel_count: int = 14,
    steps: int = 8,
    anomaly_ratio: float = 0.35,
    region: tuple[float, float] = (1.25, 103.80),
) -> dict[str, Any]:
    """Seed synthetic moving vessels and anomalies into the database.

    Returns a summary dict with counts.
    """
    rng = random.Random(42)
    base_lat, base_lon = region
    settings = get_settings()
    inserted_events = 0
    inserted_anomalies = 0
    vessel_types = ["Cargo", "Tanker", "Passenger", "Fishing", "Tug"]
    now = datetime.now(UTC).replace(microsecond=0)

    with _connect() as conn, conn.cursor() as cursor:
        for i in range(vessel_count):
            mmsi = 500000000 + i + 1
            vtype = rng.choice(vessel_types)
            lat = base_lat + rng.uniform(-0.4, 0.4)
            lon = base_lon + rng.uniform(-0.4, 0.4)
            course = rng.uniform(0, 360)
            speed = rng.uniform(4, 16)
            is_anomaly = rng.random() < anomaly_ratio
            # Anomalous vessel: occasional speed spike / sharp turn / jump.
            events = []
            for step in range(steps):
                dt = timedelta(minutes=step * 5)
                # movement along course
                dist_nm = speed * (5 / 60.0)
                angular = dist_nm / 3440.065
                bearing = math.radians(course)
                lat1, lon1 = math.radians(lat), math.radians(lon)
                lat2 = math.asin(
                    math.sin(lat1) * math.cos(angular)
                    + math.cos(lat1) * math.sin(angular) * math.cos(bearing)
                )
                lon2 = lon1 + math.atan2(
                    math.sin(bearing) * math.sin(angular) * math.cos(lat1),
                    math.cos(angular) - math.sin(lat1) * math.sin(lat2),
                )
                lat, lon = math.degrees(lat2), (math.degrees(lon2) + 540) % 360 - 180
                cur_speed = speed
                cur_course = course
                if is_anomaly and step == steps // 2:
                    # sharp course change
                    course = (course + 120) % 360
                    cur_course = course
                if is_anomaly and step == steps - 1:
                    # invalid jump: teleport
                    lat += 0.5
                    lon += 0.5
                    cur_speed = 95.0
                event_time = now - timedelta(minutes=(steps - step) * 5)
                event_id = f"sim:{mmsi}:{event_time.isoformat()}"
                events.append(
                    (event_id, "simulator", mmsi, event_time, event_time, lon, lat, cur_speed, cur_course, vtype)
                )
                inserted_events += 1

            # Insert events
            for ev in events:
                event_id, source, mm, et, ia, lo, la, sp, co, vt = ev
                cursor.execute(
                    """
                    INSERT INTO ais_event_clean (
                        event_id, source, mmsi, event_time, ingested_at,
                        geom, sog_knots, cog_degrees, heading_degrees, nav_status, vessel_type
                    ) VALUES (%s, %s, %s, %s, %s, ST_SetSRID(ST_MakePoint(%s, %s), 4326), %s, %s, %s, %s, %s)
                    ON CONFLICT (event_id) DO NOTHING
                    """,
                    (event_id, source, mm, et, ia, lo, la, sp, co, int(co), "Under way using engine", vt),
                )
                cursor.execute(
                    """
                    INSERT INTO vessel_track_point (
                        event_id, mmsi, event_time, geom, sog_knots, cog_degrees
                    ) VALUES (%s, %s, %s, ST_SetSRID(ST_MakePoint(%s, %s), 4326), %s, %s)
                    ON CONFLICT (event_id) DO NOTHING
                    """,
                    (event_id, mm, et, lo, la, sp, co),
                )

            # vessel_latest from last event
            last = events[-1]
            _, _, mm, et, ia, lo, la, sp, co, vt = last
            cursor.execute(
                """
                INSERT INTO vessel_latest (mmsi, event_time, geom, sog_knots, cog_degrees, vessel_type, updated_at)
                VALUES (%s, %s, ST_SetSRID(ST_MakePoint(%s, %s), 4326), %s, %s, %s, now())
                ON CONFLICT (mmsi) DO UPDATE SET
                    event_time = EXCLUDED.event_time, geom = EXCLUDED.geom,
                    sog_knots = EXCLUDED.sog_knots, cog_degrees = EXCLUDED.cog_degrees,
                    vessel_type = EXCLUDED.vessel_type, updated_at = now()
                WHERE EXCLUDED.event_time > vessel_latest.event_time
                """,
                (mm, et, lo, la, sp, co, vt),
            )

            if is_anomaly:
                # Record an anomaly at the jump/sharp-turn event
                ax, ay = events[-1][5], events[-1][6]
                cursor.execute(
                    """
                    INSERT INTO anomaly (mmsi, event_time, anomaly_type, severity, explanation, geom, evidence_json)
                    VALUES (%s, %s, %s, %s, %s, ST_SetSRID(ST_MakePoint(%s, %s), 4326), %s::jsonb)
                    """,
                    (
                        mm,
                        events[-1][3],
                        "sharp_course_change" if True else "invalid_jump",
                        "high" if is_anomaly else "medium",
                        f"Simulated anomaly for vessel {mm}: abnormal course/speed behaviour detected.",
                        ax,
                        ay,
                        '{"source":"simulator","rule":"simulated"}',
                    ),
                )
                inserted_anomalies += 1

    return {
        "vessels": vessel_count,
        "events": inserted_events,
        "anomalies": inserted_anomalies,
        "region": region,
    }
PYEOF
echo done