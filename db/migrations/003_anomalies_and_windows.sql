CREATE TABLE IF NOT EXISTS anomaly (
    id BIGSERIAL PRIMARY KEY,
    mmsi INTEGER NOT NULL,
    event_time TIMESTAMPTZ NOT NULL,
    anomaly_type TEXT NOT NULL,
    severity TEXT NOT NULL,
    explanation TEXT NOT NULL,
    geom geometry(Point, 4326),
    evidence_json JSONB NOT DEFAULT '{}'::jsonb
);

CREATE INDEX IF NOT EXISTS anomaly_mmsi_event_time_idx ON anomaly (mmsi, event_time);
CREATE INDEX IF NOT EXISTS anomaly_geom_idx ON anomaly USING GIST (geom);

CREATE TABLE IF NOT EXISTS window_metric (
    window_start TIMESTAMPTZ NOT NULL,
    window_end TIMESTAMPTZ NOT NULL,
    metric_name TEXT NOT NULL,
    dimensions_json JSONB NOT NULL,
    value DOUBLE PRECISION NOT NULL,
    PRIMARY KEY (window_start, window_end, metric_name, dimensions_json)
);

CREATE INDEX IF NOT EXISTS window_metric_window_start_idx ON window_metric (window_start);
CREATE INDEX IF NOT EXISTS window_metric_metric_name_idx ON window_metric (metric_name);
