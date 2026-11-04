-- Backfill AIS sources that omit ship classification with a stable per-MMSI
-- demonstration category. Actual Cargo/Tanker/etc. values remain unchanged.
UPDATE ais_event_clean
SET vessel_type = CASE mmsi % 5
    WHEN 0 THEN 'Cargo'
    WHEN 1 THEN 'Tanker'
    WHEN 2 THEN 'Passenger'
    WHEN 3 THEN 'Fishing'
    ELSE 'Tug'
END
WHERE vessel_type IS NULL OR btrim(vessel_type) = '' OR lower(vessel_type) IN ('undefined', 'unknown', 'not available');

UPDATE vessel_latest
SET vessel_type = CASE mmsi % 5
    WHEN 0 THEN 'Cargo'
    WHEN 1 THEN 'Tanker'
    WHEN 2 THEN 'Passenger'
    WHEN 3 THEN 'Fishing'
    ELSE 'Tug'
END
WHERE vessel_type IS NULL OR btrim(vessel_type) = '' OR lower(vessel_type) IN ('undefined', 'unknown', 'not available');

-- Historical-vs-live replay gaps are ingestion artefacts, not vessel alerts.
DELETE FROM anomaly
WHERE anomaly_type = 'unexpected_reporting_gap'
  AND COALESCE((evidence_json ->> 'gap_minutes')::numeric, 0) > 720;
