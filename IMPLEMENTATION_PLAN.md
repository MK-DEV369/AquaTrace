# Full Project Implementation Plan: Real-Time Marine Vessel Tracking and Route Analytics

## 1. Purpose and delivery boundary

Build a demonstrable, reproducible streaming application that receives AIS vessel-position messages, validates and enriches them, maintains current vessel state, detects simple route/speed anomalies, stores track history, and exposes the results through an API and dashboard.

This is a complete student-project roadmap, not a maritime-safety production system. The team must first prove a small, observable end-to-end pipeline with replayed public data. Live AIS, trajectory/ETA machine learning, clustering, and RAG are deliberate later increments, not shortcuts around the core pipeline.

### Phase 1 objectives

1. Ingest replayed AIS data and, when a key is available, a bounded live AIS stream.
2. Clean invalid, duplicate, stale, and out-of-order messages.
3. Partition events by MMSI, maintain per-vessel recent state, and calculate windowed metrics.
4. Persist latest position, raw/clean events, tracks, and anomaly events in PostgreSQL/PostGIS.
5. Display vessel locations, a selected track, aggregate metrics, and flagged anomalies.
6. Deliver a repeatable local deployment, automated tests, and a measurement report.

### Non-goals for the complete project

- Safety-critical collision predictions or operational vessel guidance.
- Global, unfiltered live coverage and 24/7 availability.
- Deep-learning trajectory/ETA models and an LLM/RAG interface.
- Kubernetes, multi-region failover, and a managed cloud production deployment.

## 2. Scope, assumptions, and success criteria

Use one fixed geographic bounding box (for example, Singapore Strait **or** a chosen Indian coastal port) and one historical AIS sample. Historical replay is the standard demo mode because it is reproducible; live AIS is optional and must fail gracefully if an API key or network is unavailable.

| Outcome | Minimum acceptance criterion |
|---|---|
| Reproducible startup | `docker compose up` starts the required services; one documented command starts the application. |
| End-to-end flow | At least 20,000 replayed events reach Kafka, Spark, PostGIS, REST API, and dashboard. |
| Data quality | Invalid coordinates are rejected; duplicate events are deduplicated; event-time ordering is handled with a documented watermark. |
| Fresh vessel state | The latest position of a selected vessel appears in the API and dashboard within 10 seconds during local replay. |
| Analytics | 5-minute vessel count and average speed are computed; at least two rule-based anomalies are emitted. |
| Query experience | On the project dataset, `/vessels/{mmsi}/track` has p95 response time below 1.5 seconds. |
| Evidence | Tests, sample data, screenshots, known limitations, and measured throughput/latency are included in the final report. |

Treat these as local-demo targets, not guaranteed real-world SLAs. Record the laptop specifications, sample size, replay rate, and method with every benchmark.

## 3. Architecture

```text
Historical AIS CSV ─┐
                    ├─> Python producer ─> Kafka: ais.raw
AISStream WebSocket ┘                         │
                                              v
                                  Spark Structured Streaming
                                  validate / deduplicate / watermark
                                      │        │          │
                      Kafka: ais.clean ┘        │          └─> Kafka: ais.anomalies
                                                 v
                                        PostgreSQL + PostGIS
                                   raw events, latest state, tracks,
                                   aggregates, anomalies
                                                 │
                                  FastAPI REST + WebSocket service
                                                 │
                                      Streamlit dashboard
```

### Why this design

- **Kafka** separates data arrival from processing, gives replayable topics, and makes producer/consumer responsibilities easy to demonstrate.
- **Spark Structured Streaming** is the right Phase 1 engine because it supports Kafka as a streaming source and provides event-time windows, watermarks, and checkpointing.
- **PostGIS** is simpler than a separate geospatial service for a student project; it supports spatial types/indexes and keeps the serving data in one database.
- **FastAPI + Streamlit** gives a clean API and useful dashboard with far less front-end work than a custom React application.
- **Rule-based anomalies first** make outputs explainable and testable. Isolation Forest can be added only after clean labelled/validated features exist.

## 4. Data sources and API plan

### External sources

| Source | Use | Integration | Constraints / fallback |
|---|---|---|---|
| AISStream | Optional live demo feed | Secure WebSocket connection to `wss://stream.aisstream.io/v0/stream`; subscribe with API key and geographic bounding boxes. | Keep the key in `.env`, never commit it. Rate, coverage, and terms can change. If unavailable, use replay. |
| NOAA/Marine Cadastre AccessAIS | Historical/offline training and replay data | Download a small, documented geographic/time subset; normalize to the canonical schema. | Bulk data is the practical route, not a guaranteed live API. Retain attribution and follow source terms. |
| Local AIS replay file | Required deterministic demo source | CSV/JSONL fixture in `data/sample/`; producer sends records at controlled rate. | Ship only a small permitted sample or a download script plus instructions. |

Use **one canonical event contract** regardless of source:

```json
{
  "event_id": "source:mmsi:event_time:sequence",
  "source": "replay|aisstream",
  "mmsi": 123456789,
  "event_time": "2026-10-19T09:30:15Z",
  "ingested_at": "2026-10-19T09:30:16Z",
  "latitude": 1.2914,
  "longitude": 103.8560,
  "sog_knots": 12.4,
  "cog_degrees": 87.2,
  "heading_degrees": 88,
  "nav_status": "Under way using engine",
  "vessel_type": "Cargo"
}
```

Reject or quarantine messages with missing/invalid MMSI, latitude outside `[-90, 90]`, longitude outside `[-180, 180]`, non-parseable timestamp, or impossible speed. Keep the original payload and rejection reason for auditability.

### Internal service API

Implement these endpoints after database persistence is working. Publish OpenAPI automatically through FastAPI at `/docs`.

| Endpoint | Purpose |
|---|---|
| `GET /health` | Application and dependency health. |
| `GET /metrics/summary?window_minutes=5` | Active vessels, event rate, mean speed, and anomaly count. |
| `GET /vessels?bbox=&limit=` | Latest known vessel state in a map viewport. |
| `GET /vessels/{mmsi}` | Latest position and vessel metadata. |
| `GET /vessels/{mmsi}/track?from=&to=` | Ordered historical track as GeoJSON. |
| `GET /anomalies?from=&to=&type=&bbox=` | Filterable anomaly feed. |
| `WS /ws/updates` | Small, throttled updates for dashboard refresh. |

Do not expose raw live-source credentials through this API. For a classroom demo, read-only endpoints can remain unauthenticated; document that production use needs OAuth/API-key authentication, rate limiting, CORS allow-lists, TLS, and audit logs.

## 5. Tools, services, and packages

Pin compatible versions in `pyproject.toml` and the Docker images after the first successful integration test. Do not blindly upgrade Spark/Kafka artifacts independently: the Spark Kafka connector must match the Spark/Scala build.

| Layer | Required tool/package | Purpose |
|---|---|---|
| Runtime | Python 3.12, Java 17 | Python services and Spark runtime. |
| Local orchestration | Docker Desktop, Docker Compose | One-command local stack. |
| Event broker | Apache Kafka in KRaft mode, Kafka UI (optional) | Topics, replay, and inspection without ZooKeeper. |
| Producer | `confluent-kafka`, `websockets`, `pydantic-settings`, `python-dotenv` | Reliable Kafka producer, live WebSocket client, configuration. |
| Streaming | Apache Spark, `pyspark`, matching `spark-sql-kafka-0-10` connector | Cleaning, stateful windows, watermarks, and Kafka integration. |
| Storage | PostgreSQL 16 + PostGIS 3, `psycopg[binary]`, SQLAlchemy, Alembic | Geospatial serving data and controlled migrations. |
| API | `fastapi`, `uvicorn[standard]`, `pydantic`, `httpx` | Typed REST/WebSocket API and integration tests. |
| Dashboard | `streamlit`, `pydeck` or `plotly`, `requests` | Map, metrics, tracks, and anomaly views. |
| Analysis | `pandas`, `numpy`, `geopandas`, `shapely`, `pyarrow` | Offline EDA and spatial feature preparation. |
| Quality | `pytest`, `pytest-asyncio`, `testcontainers`, `ruff`, `mypy`, `pre-commit` | Tests, linting, type checking, reproducibility. |
| Observability | `prometheus-client` (optional), structured JSON logging | Event rate, lag, errors, and demo diagnostics. |

Optional later packages: `scikit-learn` for Isolation Forest and DBSCAN; `mlflow` for tracked experiments; `sentence-transformers` plus `pgvector` for RAG. Do not add these to the MVP until the core system is demonstrably reliable.

## 6. Data model and Kafka topics

### Topics

| Topic | Key | Retention/use |
|---|---|---|
| `ais.raw` | MMSI | Immutable source-normalized events; short replay retention. |
| `ais.clean` | MMSI | Valid, deduplicated events consumed by analytics. |
| `ais.invalid` | event ID | Quarantined events plus validation reason. |
| `ais.anomalies` | MMSI | Compact, explainable anomaly notifications. |

Start with 3 partitions and replication factor 1 locally. Key all vessel events by MMSI so that one vessel’s ordered state stays in one Kafka partition. Use JSON in Phase 1; add Schema Registry/Avro only if schema evolution becomes a real need.

### Serving tables

- `ais_event_raw(event_id, source, payload, ingested_at)`
- `ais_event_clean(event_id, mmsi, event_time, geom, sog_knots, cog_degrees, vessel_type, ...)`
- `vessel_latest(mmsi PRIMARY KEY, event_time, geom, speed, course, updated_at, ...)`
- `vessel_track_point(mmsi, event_time, geom, ...)`, indexed on `(mmsi, event_time)` and `geom GIST`
- `anomaly(id, mmsi, event_time, anomaly_type, severity, explanation, geom, evidence_json)`
- `window_metric(window_start, window_end, metric_name, dimensions_json, value)`

Use `geometry(Point, 4326)` and store timestamps in UTC. The database update must be idempotent: the same `event_id` never creates duplicate state or track rows.

## 7. Processing rules

1. Parse source payload into the canonical schema and attach `ingested_at`.
2. Validate required fields/ranges and publish failures to `ais.invalid`.
3. Deduplicate by source/MMSI/event time/position with a bounded event-time watermark (start at 10 minutes; tune from observed delay).
4. Drop events too far out of order, count them, and retain them in raw storage for review.
5. Within each MMSI, compute previous position/time, distance, speed change, heading change, and reporting gap.
6. Create 5-minute tumbling metrics: active vessels, message count, mean/median speed, vessel-type count, and anomaly count.
7. Upsert `vessel_latest` only if an event is newer than the current state.

### MVP anomaly rules

- **Invalid jump:** implied speed between adjacent positions is above a documented threshold (start with 60 knots; calibrate by vessel class).
- **Unexpected reporting gap:** an underway vessel has no message for more than 30 minutes.
- **Optional:** speed is above a vessel-class threshold or course changes sharply within a short interval.

Every alert must carry the calculation and threshold that caused it. Avoid presenting a rule-based alert as proof of illegal behavior, spoofing, or collision risk.

## 8. Roadmap: core build (weeks 1-8)

| Week | Milestone and deliverables | Exit check |
|---|---|---|
| 1 | Repository scaffold, architecture decision record, Docker Compose for Kafka/PostGIS, sample dataset profile, canonical schema. | Services start cleanly; schema and source licences are documented. |
| 2 | Replay producer to `ais.raw`; source normalizer; topic inspection. | Replay 20,000 deterministic events and preserve source offsets. |
| 3 | Spark clean stream, validation/quarantine, deduplication, watermark, checkpointing. | Unit/integration tests show expected valid/invalid/duplicate counts. |
| 4 | PostGIS schema/migrations and idempotent clean-stream sink. | Latest state and historical tracks survive service restart. |
| 5 | Window aggregates and two rule-based anomalies; benchmark harness. | Metrics and seeded anomaly fixtures are correct. |
| 6 | FastAPI read endpoints, OpenAPI docs, and dashboard map/track/metrics views. | Dashboard matches API/database results for selected MMSIs. |
| 7 | Optional bounded AISStream adapter; observability; fault/restart testing; performance tuning. | Replay works without loss/duplicate serving rows after a controlled restart. |
| 8 | Documentation, demo script, screenshots, test report, final presentation, and retrospective. | A teammate can reproduce the demo from a clean checkout. |

### Work priority if time runs short

1. Replay -> Kafka -> Spark -> PostGIS.
2. Validation/deduplication and a correct vessel/track API.
3. Dashboard and two explainable alerts.
4. Live source adapter.
5. ML, clustering, ETA, and natural-language querying.

This ordering protects the project from the most common failure mode: attractive advanced features built on an unreliable or unmeasurable pipeline.

## 9. Advanced analytics and product roadmap (weeks 9-16)

### Phase 2: predictive and route analytics (weeks 9-11)

| Capability | Implementation | Evaluation / acceptance criterion |
|---|---|---|
| Feature store | Materialize lagged speed/course, reporting gap, turn rate, distance-to-port, vessel class, and rolling-window features to Parquet/PostGIS. | No feature may use information after its prediction timestamp. |
| ETA baseline | Start with a deterministic baseline: remaining distance / robust recent speed. | Report MAE and median absolute error on a chronological held-out set. |
| Trajectory baseline | Predict the next 15/30/60 minutes with constant velocity/course. | Report median and p95 geodesic error against held-out tracks. |
| ML trajectory/ETA | Compare a compact sequence model only if it beats the baseline consistently. Store model/version/metrics with MLflow. | Improvement over baseline must be statistically and operationally meaningful; otherwise retain the baseline. |
| ML anomaly detection | Train Isolation Forest on clean, engineered behaviour features; emit score and feature evidence. | Compare against seeded cases and manually reviewed samples; never label a score as criminal activity. |
| Route discovery | Cluster resampled historical tracks with DBSCAN/HDBSCAN; store cluster and confidence. | Clusters must be stable across time samples and visual inspection. |

Use time-based train/validation/test splits grouped by vessel where appropriate. Prevent track fragments from the same future voyage leaking into train and test. Deep LSTM/Transformer models are research extensions, not a completion requirement.

### Phase 3: natural-language analytics (weeks 12-13)

Build the query assistant only over a curated data dictionary, methodology, and pre-approved analytical tools. Do **not** connect an LLM directly to unrestricted SQL or live Kafka commands.

1. Index project documentation, API descriptions, metric definitions, and known limitations in `pgvector`.
2. Provide read-only, parameterized tools such as `get_vessel`, `get_track_summary`, `list_anomalies`, and `get_window_metrics`.
3. Route each question to retrieval, a safe analytical tool, or a refusal when its requested conclusion exceeds data evidence.
4. Return citations to the data/metric window, prompt/tool audit records, and an explicit uncertainty statement.
5. Evaluate with a fixed question set: factual retrieval accuracy, tool-call correctness, refusal safety, latency, and unsupported-claim rate.

Suggested packages: `pgvector`, `sentence-transformers`, `langchain` or a small provider SDK wrapper, and `pydantic` tool schemas. Use an API key only from server-side environment variables. The assistant is a convenience layer and must not be required for the core demo.

### Phase 4: hardening and optional deployment (weeks 14-16)

| Area | Completion work |
|---|---|
| Security | OAuth/API-key authentication, roles, secret manager, TLS, CORS allow-list, input limits, rate limiting, and dependency scanning. |
| Reliability | Dead-letter processing, retry policy, backup/restore drill, Kafka retention policy, database indexes, and dashboards for lag/errors. |
| Delivery | CI pipeline for lint/type/test/image build, tagged releases, environment-specific configuration, and reproducible benchmark jobs. |
| Deployment | Deploy the containerized API/dashboard and managed database/broker only after cost limits, access controls, data-source terms, and monitoring are agreed. |
| Governance | Data retention, attribution, access log, model cards, evaluation report, and a clear statement that analytics are not navigation or enforcement decisions. |

## 10. Repository layout

```text
ship/
  docker-compose.yml
  pyproject.toml
  .env.example
  README.md
  data/sample/                  # small permitted replay fixture only
  docs/                         # ADRs, source/licence notes, benchmark results
  services/
    producer/                   # replay and optional AISStream adapters
    stream_processor/           # Spark job(s)
    api/                        # FastAPI service
    dashboard/                  # Streamlit application
  db/migrations/                # Alembic/PostGIS migrations
  tests/                        # unit, integration, contract tests
  scripts/                      # bootstrap, replay, verification commands
```

Commit `.env.example`, never `.env`, API keys, real credentials, or large source downloads. Use a secret scanner in pre-commit and CI.

## 11. Testing, observability, and delivery

### Tests

- Unit: parsing, validation ranges, event IDs, deduplication keys, distance/speed calculation, anomaly thresholds.
- Integration: Kafka producer -> Spark -> PostGIS using a fixed 100- to 1,000-event fixture.
- API contract: response validation and GeoJSON track ordering.
- Resilience: restart producer/processor during replay; prove idempotent writes and checkpoint recovery.
- Load: replay at 1x, 5x, and 10x; capture producer rate, consumer lag, Spark batch duration, sink errors, and API latency.

### Demo script

1. Start the stack and check `/health`.
2. Start a deterministic 20,000-event replay.
3. Show `ais.raw`, `ais.clean`, and `ais.invalid` topic counts.
4. Open the dashboard, select a vessel, and inspect its track and 5-minute metrics.
5. Show a seeded invalid-jump/gap alert and its explanation.
6. Restart the processing service and show that latest state remains correct.

### Required final artifacts

- README with prerequisites, setup, configuration, commands, and teardown.
- Architecture diagram and data-flow explanation.
- Data dictionary, source attribution/licensing note, and privacy/ethical limitations.
- API examples/OpenAPI link and screenshots.
- Test results plus performance table with methodology.
- Known limitations and a Phase 2 backlog (ML, route clustering, ETA, RAG).

## 12. Risks and mitigations

| Risk | Mitigation |
|---|---|
| Live AIS key/feed fails | Make replay the required demo path; keep the live adapter optional. |
| Spark/Kafka dependency mismatch | Containerize versions and validate the matching Spark Kafka connector in Week 1. |
| Out-of-order/duplicate data produces wrong tracks | Use event time, MMSI partitioning, watermarking, idempotent event IDs, and fixtures that prove each case. |
| Dataset is too large for a laptop | Begin with a bounded time/area subset; store only required columns; use Parquet for offline work. |
| ML looks impressive but cannot be evaluated | Defer it until baseline rules and labelled/validated features exist; compare every model to the baseline. |
| Confidential API keys enter Git | `.env`, `.gitignore`, secret scanning, and immediate key rotation if exposed. |

## 13. Reference implementation documentation

- AISStream WebSocket documentation: https://www.aisstream.io/documentation/
- NOAA/Marine Cadastre AccessAIS: https://marinecadastre.gov/accessais/
- Spark Structured Streaming Kafka integration: https://spark.apache.org/docs/latest/structured-streaming-kafka-integration.html
- Confluent Kafka Python client: https://docs.confluent.io/kafka-clients/python/current/overview.html

