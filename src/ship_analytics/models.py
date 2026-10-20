"""Database schema definitions for Ship Analytics."""

from geoalchemy2 import Geography
from sqlalchemy import (
    JSON,
    BigInteger,
    Column,
    DateTime,
    Float,
    Integer,
    MetaData,
    PrimaryKeyConstraint,
    String,
    Table,
)

metadata = MetaData()


AISEventClean = Table(
    "ais_event_clean",
    metadata,
    Column("event_id", String, primary_key=True),
    Column("source", String, nullable=False),
    Column("mmsi", Integer, nullable=False),
    Column("event_time", DateTime, nullable=False),
    Column("ingested_at", DateTime, nullable=False),
    Column("geom", Geography("POINT", srid=4326), nullable=False),
    Column("sog_knots", Float),
    Column("cog_degrees", Float),
    Column("heading_degrees", Integer),
    Column("nav_status", String),
    Column("vessel_type", String),
)


VesselLatest = Table(
    "vessel_latest",
    metadata,
    Column("mmsi", Integer, primary_key=True),
    Column("event_time", DateTime, nullable=False),
    Column("geom", Geography("POINT", srid=4326), nullable=False),
    Column("sog_knots", Float),
    Column("cog_degrees", Float),
    Column("vessel_type", String),
    Column("updated_at", DateTime, nullable=False),
)


VesselTrackPoint = Table(
    "vessel_track_point",
    metadata,
    Column("event_id", String, primary_key=True),
    Column("mmsi", Integer, nullable=False),
    Column("event_time", DateTime, nullable=False),
    Column("geom", Geography("POINT", srid=4326), nullable=False),
    Column("sog_knots", Float),
    Column("cog_degrees", Float),
)


Anomaly = Table(
    "anomaly",
    metadata,
    Column("id", BigInteger, primary_key=True),
    Column("mmsi", Integer, nullable=False),
    Column("event_time", DateTime, nullable=False),
    Column("anomaly_type", String, nullable=False),
    Column("severity", String, nullable=False),
    Column("explanation", String, nullable=False),
    Column("geom", Geography("POINT", srid=4326)),
    Column("evidence_json", JSON, server_default='{}'),
)


WindowMetric = Table(
    "window_metric",
    metadata,
    Column("window_start", DateTime, nullable=False),
    Column("window_end", DateTime, nullable=False),
    Column("metric_name", String, nullable=False),
    Column("dimensions_json", JSON, nullable=False),
    Column("value", Float, nullable=False),
    PrimaryKeyConstraint("window_start", "window_end", "metric_name", "dimensions_json"),
)
