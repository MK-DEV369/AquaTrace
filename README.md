# Ship Analytics

Real-time AIS vessel tracking and route analytics. The local starter stack contains Kafka, PostGIS, a replay producer, a Spark processing job, a FastAPI read API, and a Streamlit dashboard.

The build order is intentional: use deterministic replay before connecting optional live AIS credentials. See [the full implementation plan](IMPLEMENTATION_PLAN.md) for scope, architecture, milestones, external source considerations, and evaluation criteria.

## Status

- **Phase 1**: Core AIS analytics pipeline — **COMPLETE** (21/21 unit tests passing)
- **Phase 2**: Advanced analytics & ML — **COMPLETE** (all 4 components import successfully, 7 API routes)
- **Phase 3**: Enhanced natural language — **PLANNED**
- **Phase 4**: Production hardening — **PLANNED**

## Prerequisites

- Docker Desktop with Docker Compose (for Kafka + PostgreSQL + PostGIS)
- Python 3.12–3.13
- Java 17 (required only for the Spark job)
- NVIDIA GPU with CUDA 12.4 driver (for torch + sentence-transformers)
- An AISStream API key only when testing the optional live adapter

## Quick start

### 1. Build & run the Docker stack (no cache, CUDA torch)

```powershell
# Remove old containers and images
docker compose down -v --rmi all

# Rebuild from scratch — pulls CUDA torch from the PyTorch index
docker compose build --no-cache api
docker compose up -d

# Verify all 4 containers are running
docker ps
```

The `docker compose build` step installs all dependencies including `torch>=2.6.0` from `https://download.pytorch.org/whl/cu124` (CUDA build, no CPU fallback). This avoids the pip resolver pulling the CPU wheel.

### 2. Start the backend (API)

The API container starts automatically via Docker Compose. To run it manually:

```powershell
# Inside the container or with host deps installed
uvicorn services.api.main:app --host 0.0.0.0 --port 8000 --reload
```

Then open:
- API/OpenAPI docs: http://localhost:8000/docs
- Health check: http://localhost:8000/health
- Phase 2 detailed health: http://localhost:8000/health/detailed

Phase 2 endpoints:
| Method | Path | Description |
|--------|------|-------------|
| GET | `/health/detailed` | Deep health check (DB, Kafka, ML, NLP) |
| GET | `/eta/predictions` | ETA predictions for vessels |
| POST | `/ml/anomaly-detection` | Train Isolation Forest anomaly detector |
| GET | `/routes/discover` | Discover frequent maritime routes |
| POST | `/natural-language/query` | Natural language query with NLP |
| POST | `/natural-language/register-tool` | Register NLP tools |
| GET | `/ws/advanced-updates` | WebSocket for advanced updates |

### 3. Start the frontend (dashboard)

The dashboard container starts automatically via Docker Compose. To run it manually:

```powershell
streamlit run services/dashboard/app.py --server.port 8501 --server.address 0.0.0.0
```

Then open: http://localhost:8501

### 4. Replay sample data (producer)

```powershell
python -m services.producer.replay --file data/sample/ais_events.jsonl --rate 2
```

### 5. Start the Spark stream processor

The Spark processor runs in its own Docker container, which avoids Windows
Hadoop `winutils.exe` requirements and connects to Kafka using the Docker
network hostname:

```powershell
docker compose up -d spark
docker compose logs -f spark
```

To start the complete stack, including Spark:

```powershell
docker compose up -d
```

## Running without Docker

Unit tests for core logic work without Docker:

```powershell
python -m pytest tests/ -v
```

## Project structure

```
Ship/
├── docker-compose.yml          # Kafka, PostGIS, API, Dashboard
├── Dockerfile                  # API image with CUDA torch
├── pyproject.toml              # Dependencies (torch from CUDA index)
├── quickstart.ps1 / .sh        # One-command build + run + test
├── src/ship_analytics/         # Core library
│   ├── config.py               # Settings & env
│   ├── schemas.py              # Pydantic models
│   ├── anomalies.py            # Anomaly detection
│   ├── windows.py              # Time-window calculations
│   ├── models.py               # SQLAlchemy ORM
│   ├── kafka.py                # Kafka producer/consumer
│   ├── etabaselines.py         # Phase 2: ETA baselines
│   ├── feature_store.py        # Phase 2: Feature engineering
│   ├── mlmodels.py             # Phase 2: ML models (Isolation Forest)
│   └── natural_language.py     # Phase 2: NLP processing
├── services/
│   ├── api/
│   │   ├── main.py             # FastAPI app (v0.2.0)
│   │   └── phase2.py           # Phase 2 API router (7 routes)
│   ├── dashboard/              # Streamlit dashboard
│   │   └── app.py
│   ├── producer/               # AIS data producers
│   │   ├── replay.py           # Replay from JSONL
│   │   └── aisstream.py       # Live AISStream adapter
│   └── stream_processor/       # Spark streaming job
│       └── job.py
├── db/migrations/              # SQL migrations
├── tests/                      # Test suite (21 passing, 1 skipped)
└── data/sample/                # Sample AIS data
```

## Configuration

Copy `.env.example` to `.env` and set your AISStream API key:

```powershell
Copy-Item .env.example .env
```

Required env vars:
- `AISSTREAM_API_KEY` — AISStream API key (optional, for live adapter)
- `DATABASE_URL` — PostgreSQL connection string
- `KAFKA_BOOTSTRAP_SERVERS` — Kafka bootstrap servers

## Testing

```powershell
python -m pytest tests/ -v
```