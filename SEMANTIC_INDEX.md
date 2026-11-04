# AquaTrace / Ship Analytics Semantic Index

Use this index to locate project behavior by concept. Paths are relative to the repository root. The code is the source of truth; phase summaries and reports describe intended/current status but may lag implementation.

## Product and system entry points

- **Project overview, API catalog, investigation workflow:** `README.md`
- **End-to-end setup and operational commands:** `quickstart.ps1`, `quickstart.sh`, `docker-compose.yml`, `Dockerfile`, `Dockerfile.spark`
- **Python dependencies, extras, lint/test settings:** `pyproject.toml`
- **Environment variables and runtime configuration:** `src/ship_analytics/config.py`
- **Implementation history and milestone summaries:** `PHASE1_COMPLETE.md`, `PHASE2_COMPLETE.md`, `PHASE1_IMPLEMENTATION.md`, `ENHANCED_PHASE_1_2_IMPLEMENTATION.md`, `ENHANCED_PHASE_1_2_IMPLEMENTATION_SUMMARY.md`, `FINAL_IMPLEMENTATION_SUMMARY.md`
- **Known problems / security review:** `ERRORS.md`, `SECURITY-REVIEW.md`

## Data flow and streaming

- **Canonical AIS event contract and validation:** `src/ship_analytics/schemas.py`
- **Kafka topics, clients, and integration lifecycle:** `src/ship_analytics/kafka.py`, `services/kafka_integration.py`
- **Replay historical/local AIS input:** `services/producer/replay.py`
- **Connect to AISStream WebSocket:** `services/producer/aisstream.py`
- **Spark Structured Streaming pipeline:** `services/stream_processor/job.py`
- **Kafka message to event model / WebSocket contracts:** `services/websocket/schemas.py`
- **WebSocket connection lifecycle and message handling:** `services/websocket/connection_manager.py`, `services/websocket/handler.py`
- **Sample input and deterministic integration fixtures:** `data/sample/ais_events.jsonl`, `tests/fixtures/six_events.jsonl`

## Storage and serving

- **PostgreSQL/PostGIS ORM models:** `src/ship_analytics/models.py`
- **Database connection/session helpers:** `services/api/db.py`
- **Initial PostGIS extension/schema:** `db/init/001_extensions.sql`, `db/init/002_schema.sql`
- **Anomaly/window migration:** `db/migrations/003_anomalies_and_windows.sql`
- **API routes, health, vessel state, tracks, metrics:** `services/api/main.py`
- **Phase 2 API routes for prediction/analytics/query:** `services/api/phase2.py`
- **API orchestration of agentic queries:** `services/api/agent_router.py`
- **Interactive map and metrics dashboard:** `services/dashboard/app.py`

## Analytics and models

- **Rule-based anomaly types and thresholds:** `src/ship_analytics/anomalies.py`
- **Windowed aggregation and event-time metrics:** `src/ship_analytics/windows.py`, `src/ship_analytics/metrics.py`
- **Temporal/spatial feature generation and persistence:** `src/ship_analytics/feature_store.py`
- **ETA baseline calculations:** `src/ship_analytics/etabaselines.py`
- **Trajectory prediction models:** `src/ship_analytics/trajectory_models.py`
- **General ML training/inference (Isolation Forest, route clustering):** `src/ship_analytics/mlmodels.py`
- **Advanced DBSCAN route discovery:** `src/ship_analytics/advanced_dbscan.py`
- **Model abstractions and shared types:** `src/ship_analytics/models.py`
- **Model evaluation metrics:** `src/ship_analytics/metrics.py`, `tests/eval/metrics.py`
- **Prediction benchmark tests:** `tests/eval/test_trajectory_benchmark.py`

## Natural language and agentic investigation

- **Natural-language intent, retrieval, safe analytical tools:** `src/ship_analytics/natural_language.py`
- **LLM provider/runtime integration:** `src/ship_analytics/llm.py`
- **Agent core abstraction and shared types:** `src/ship_analytics/agents/base.py`
- **Agent implementations / specialist roles:** `src/ship_analytics/agents/specialists.py`
- **Agent registry and available roles:** `src/ship_analytics/agents/registry.py`
- **Agent orchestration/planning:** `src/ship_analytics/agents/orchestrator.py`
- **Agent runtime coordination:** `src/ship_analytics/agents/runtime.py`
- **Agent-facing investigation tools:** `services/agent/tools.py`
- **Investigation workflow and evidence synthesis:** `services/agent/investigation_agent.py`
- **Agent event history and audit data:** `services/agent/event_store.py`
- **Agent resource quotas/lifecycle:** `services/agent/resource_manager.py`
- **Transaction/session coordination:** `services/agent/transaction_manager.py`

## Security, observability, and reliability

- **JWT/authentication and zero-trust policy:** `services/auth/zero_trust.py`
- **Structured logging:** `src/ship_analytics/logging.py`
- **Runtime metrics:** `src/ship_analytics/metrics.py`
- **Agent traces and API surface:** `services/api/agent_router.py`, `src/ship_analytics/agents/`
- **Checkpointed stream recovery:** `services/stream_processor/job.py`

## Tests and validation scripts

- **AIS schemas and core behavior:** `tests/test_schemas.py`, `tests/test_phase1.py`
- **Six-event end-to-end processing:** `tests/test_e2e_six_events.py`
- **Enhanced integration coverage:** `tests/enhanced_integration_test.py`
- **Trajectory/ETA evaluation:** `tests/eval/`
- **Implementation/phase checks:** `check_implementation.py`, `validate_implementation.py`, `verify_phase1.py`, `test_agent_implementation.py`, `test_websocket_import.py`, `kafka_integration_validation.py`, `final_validation.py`, `demonstration.py`, `demo_implementation.py`

## Documentation and visual assets

- **Original proposal / course submission:** `Spa phase 1.pdf` (if present)
- **Team report:** `22_SPA_Project_Report.docx`
- **Architecture/demo figures:** `images/`
- **Dataset attribution/information:** `README_information_CSV_Danish.txt`

## Concept lookup

| If you are looking for… | Start here |
|---|---|
| How AIS records are represented | `src/ship_analytics/schemas.py` |
| How data gets into Kafka | `services/producer/` |
| How events are cleaned and analyzed in real time | `services/stream_processor/job.py`, `src/ship_analytics/anomalies.py`, `src/ship_analytics/windows.py` |
| Where the current vessel position is saved | `src/ship_analytics/models.py`, `db/init/002_schema.sql` |
| How vessel data is exposed to clients | `services/api/main.py` |
| How to view vessel positions | `services/dashboard/app.py` |
| How an incident investigation runs | `services/agent/investigation_agent.py`, `src/ship_analytics/agents/orchestrator.py` |
| How the AI is constrained to safe tools | `src/ship_analytics/natural_language.py`, `services/agent/tools.py`, `services/auth/zero_trust.py` |
| How predictions/anomaly models are built | `src/ship_analytics/feature_store.py`, `src/ship_analytics/trajectory_models.py`, `src/ship_analytics/mlmodels.py` |
| What behavior is tested | `tests/` and the validation scripts at repository root |

