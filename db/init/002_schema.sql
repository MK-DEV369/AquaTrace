CREATE TABLE IF NOT EXISTS ais_event_clean (
    event_id TEXT PRIMARY KEY,
    source TEXT NOT NULL,
    mmsi INTEGER NOT NULL,
    event_time TIMESTAMPTZ NOT NULL,
    ingested_at TIMESTAMPTZ NOT NULL,
    geom geometry(Point, 4326) NOT NULL,
    sog_knots DOUBLE PRECISION,
    cog_degrees DOUBLE PRECISION,
    heading_degrees INTEGER,
    nav_status TEXT,
    vessel_type TEXT
);

CREATE INDEX IF NOT EXISTS ais_event_clean_mmsi_event_time_idx
    ON ais_event_clean (mmsi, event_time DESC);

CREATE TABLE IF NOT EXISTS vessel_latest (
    mmsi INTEGER PRIMARY KEY,
    event_time TIMESTAMPTZ NOT NULL,
    geom geometry(Point, 4326) NOT NULL,
    sog_knots DOUBLE PRECISION,
    cog_degrees DOUBLE PRECISION,
    vessel_type TEXT,
    updated_at TIMESTAMPTZ NOT NULL
);

CREATE INDEX IF NOT EXISTS vessel_latest_geom_idx ON vessel_latest USING GIST (geom);

CREATE TABLE IF NOT EXISTS vessel_track_point (
    event_id TEXT PRIMARY KEY,
    mmsi INTEGER NOT NULL,
    event_time TIMESTAMPTZ NOT NULL,
    geom geometry(Point, 4326) NOT NULL,
    sog_knots DOUBLE PRECISION,
    cog_degrees DOUBLE PRECISION
);

CREATE INDEX IF NOT EXISTS vessel_track_point_mmsi_event_time_idx
    ON vessel_track_point (mmsi, event_time ASC);
CREATE INDEX IF NOT EXISTS vessel_track_point_geom_idx ON vessel_track_point USING GIST (geom);

CREATE TABLE IF NOT EXISTS anomaly (
    id BIGSERIAL PRIMARY KEY,
    mmsi INTEGER NOT NULL,
    event_time TIMESTAMPTZ NOT NULL,
    anomaly_type TEXT NOT NULL,
    severity TEXT NOT NULL,
    explanation TEXT NOT NULL,
    geom geometry(Point, 4326),
    evidence_json JSONB NOT NULL DEFAULT '{}'::jsonb
);

