# Ship Analytics — Known Issues & Errors

This file tracks all known issues, dependency problems, and unresolved items. Last updated: 2026-10-20.

## 1. Docker Image Build (IN PROGRESS)

**Status**: Building  
**Impact**: API container needs all Phase 2 deps (geopandas, scikit-learn, sentence-transformers, spacy)

### Changes Made
- Added `geopandas>=1.0,<2`, `scikit-learn>=1.5,<2`, `numpy>=2.0,<3`, `sentence-transformers>=3.0,<4`, `spacy>=3.8,<4` to core `dependencies` in `pyproject.toml`
- Added `python -m spacy download en_core_web_sm` to Dockerfile
- Removed `en-core-web-sm` from pip deps (not a valid PyPI package; use `spacy download` instead)
- Removed silent `try/except ImportError` around Phase 2 router import in `main.py`

### Build Command
```powershell
cd "E:\4th_Year_Data\3. Stream Processing and Analytics\Ship"
docker compose build api
docker compose up -d api
```

## 2. Java 17 Not Installed

**Status**: Unresolved  
**Impact**: Cannot run Spark stream processor locally.

Java 17 is required for Spark 3.5.5. Not installed on this machine.

### Fix
```bash
sdk install java 17.0.9-open
```

## 3. Test Results (PASSING)

**Status**: Resolved  
**Impact**: All unit tests pass.

```
21 passed, 1 skipped in 0.62s
```

The 1 skipped test requires Docker/Kafka (`test_kafka_topic_creation`).

## 4. Phase 2 API Router (FIXED)

**Status**: Resolved  
**Impact**: All 7 Phase 2 endpoints load successfully.

Endpoints:
- `GET /health/detailed` — Deep health check with DB, Kafka, ML, NLP status
- `GET /eta/predictions` — ETA predictions for vessels
- `POST /ml/anomaly-detection` — Train Isolation Forest anomaly detector
- `GET /routes/discover` — Discover frequent maritime routes
- `POST /natural-language/query` — Natural language query with NLP
- `POST /natural-language/register-tool` — Register NLP tools
- `GET /ws/advanced-updates` — WebSocket for advanced updates

## 5. Pydantic Schema Warning (FIXED)

**Status**: Resolved  
**Impact**: `schema` field name shadowed `BaseModel.schema` attribute.

Fixed by renaming `schema` → `tool_schema` in `register_natural_language_tool` endpoint.

## 6. Python Version

**Status**: Informational  
**Impact**: None.

The project specifies `>=3.12,<3.14` in `pyproject.toml`. Current environment uses Python 3.13.7, which is compatible.

## Summary

| # | Issue | Status | Blocker for |
|---|-------|--------|-------------|
| 1 | Docker image build | In Progress | API container with Phase 2 deps |
| 2 | Java 17 for Spark | Unresolved | Spark stream processor |
| 3 | Test data issues | FIXED | All unit tests (21/21 pass) |
| 4 | SQLAlchemy import | FIXED | Module imports |
| 5 | Pydantic schema | FIXED | Anomaly model |
| 6 | Phase 2 API router | FIXED | API integration (7 routes) |
| 7 | Pydantic shadow warning | FIXED | NLP register-tool endpoint |
| 8 | Python version | N/A | Compatible |

## Next Steps

1. Run `docker compose build api` then `docker compose up -d api` to rebuild container
2. Test Phase 2 endpoints: `curl http://localhost:8000/health/detailed`
3. Test full pipeline: producer → Kafka → stream processor → API
4. Install Java 17 for Spark testing