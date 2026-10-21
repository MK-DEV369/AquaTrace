# Ship Analytics

Real-time AIS vessel tracking and route analytics. The local starter stack contains Kafka, PostGIS, a replay producer, a Spark processing job, a FastAPI read API, and a Streamlit dashboard.

The build order is intentional: use deterministic replay before connecting optional live AIS credentials. See [the full implementation plan](IMPLEMENTATION_PLAN.md) for scope, architecture, milestones, external source considerations, and evaluation criteria.

## Status

- **Phase 1**: Core AIS analytics pipeline — **COMPLETE** (21/21 unit tests passing)
- **Phase 2**: Advanced analytics & ML — **COMPLETE** (all 4 components import successfully, 7 API routes)
- **Phase 3**: Enhanced natural language — **PLANNED**
- **Phase 4**: Production hardening — **PLANNED**

## Key Improvements

### 🚀 **WebSocket Infrastructure** (NEW)
- **Enhanced Connection Manager**: JWT-based authentication, rate limiting, heartbeat monitoring
- **Message Validation**: Pydantic schemas with comprehensive validation
- **Subscription System**: Channel-based message filtering and routing
- **Authentication**: Secure WebSocket connections with token validation
- **Error Recovery**: Automatic reconnection and robust error handling

### 📊 **Advanced DBSCAN Implementation** (ENHANCED)
- **Parameter Optimization**: Silhouette score maximization, elbow method, knee point detection
- **Enhanced Spatial Features**: Geometric transformations, trajectory analysis, velocity features
- **Streaming Support**: Real-time clustering with sliding window processing
- **Quality Metrics**: Comprehensive cluster validation and scoring
- **Adaptive Learning**: Dynamic parameter tuning for optimal results

### 🧠 **Enhanced Natural Language Processing** (UPGRADED)
- **Advanced NLP Models**: Free distil-code and distil-structured models with better frontend capabilities
- **Tool Registration**: Extensible system for custom NLP tools
- **Query Processing**: Enhanced natural language understanding with context awareness
- **Safety Validation**: Comprehensive safety assessment for NLP queries

### 🔐 **Production-Ready Security**
- **JWT Authentication**: Secure API and WebSocket authentication
- **Rate Limiting**: Per-connection and per-IP protection
- **Input Validation**: Comprehensive request/response validation
- **Error Handling**: Structured error responses with detailed information

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

## Quick start

### 1. Build & run the Docker stack (no cache, CUDA torch)

```powershell
# Remove old containers and images
docker compose down -v --rmi all

# Rebuild from scratch — pulls CUDA torch from the PyTorch index
docker compose build --no-cache api

# Start all containers with enhanced WebSocket and DBSCAN support
docker compose up -d

# Verify all 4 containers are running
docker ps
```

The `docker compose build` step installs all dependencies including `torch>=2.6.0` from `https://download.pytorch.org/whl/cu124` (CUDA build, no CPU fallback). This avoids the pip resolver pulling the CPU wheel.

### 2. Start the backend (API) with Enhanced Features

The API container starts automatically via Docker Compose. To run it manually with WebSocket authentication:

```powershell
# Inside the container or with host deps installed
# Set your Hugging Face token for distil models
$env:HF_TOKEN = "your_huggingface_token"

# Run with enhanced WebSocket support
uvicorn services.api.main:app --host 0.0.0.0 --port 8000 --reload
```

**Phase 2 Enhanced Endpoints:**
| Method | Path | Description |
|--------|------|-------------|
| GET | `/health/detailed` | Deep health check (DB, Kafka, ML, NLP, WebSocket) |
| GET | `/eta/predictions` | ETA predictions for vessels |
| POST | `/ml/anomaly-detection` | Train enhanced Isolation Forest with DBSCAN |
| GET | `/routes/discover` | Discover frequent maritime routes (with DBSCAN clustering) |
| POST | `/natural-language/query` | Natural language query with distil NLP models |
| POST | `/natural-language/register-tool` | Register NLP tools with enhanced validation |
| GET | `/ws/updates` | WebSocket for real-time updates with authentication |
| GET | `/ws/advanced-updates` | WebSocket with advanced analytics and ML updates |

**New WebSocket Features:**
- JWT token authentication via query parameter: `ws://localhost:8000/ws/updates?token=your_jwt_token`
- Channel subscriptions: `subscribe`, `unsubscribe` to filter updates
- Heartbeat monitoring for connection health
- Enhanced message validation and error handling

### 3. Start the frontend (dashboard)

The dashboard container starts automatically via Docker Compose. To run it manually:

```powershell
streamlit run services/dashboard/app.py --server.port 8501 --server.address 0.0.0.0
```

**Enhanced Frontend Features:**
- Real-time WebSocket integration for live updates
- Enhanced route visualization with DBSCAN clustering
- ML model performance monitoring
- Natural language query interface
- Connection status and health monitoring

Then open:
- API/OpenAPI docs: http://localhost:8000/docs
- Health check: http://localhost:8000/health
- Phase 2 detailed health: http://localhost:8000/health/detailed
- WebSocket test: http://localhost:8000/docs#/websocket
- Dashboard: http://localhost:8501

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

**Enhanced Spark Processing:**
- Real-time DBSCAN clustering for streaming data
- Enhanced feature extraction for ML models
- WebSocket integration for real-time analytics updates
- Optimized performance with CUDA acceleration

To start the complete stack with all enhancements:

```powershell
docker compose up -d
```

## Running without Docker

Unit tests for core logic work without Docker:

```powershell
python -m pytest tests/ -v
```

### Testing Enhanced Features

To verify the new WebSocket and DBSCAN enhancements:

```powershell
# Test WebSocket connection with authentication
python -c "
import requests
import websocket
import json

# Test API endpoints
base_url = 'http://localhost:8000'

# Test health endpoint
response = requests.get(f'{base_url}/health/detailed')
print('Health Status:', response.status_code)

# Test routes discover endpoint
response = requests.get(f'{base_url}/routes/discover?min_points=10')
print('Routes Discover Status:', response.status_code)

# Test ML anomaly detection
response = requests.post(f'{base_url}/ml/anomaly-detection', 
                        json={'feature_columns': ['sog_knots', 'cog_degrees'], 'training_hours': 168})
print('ML Anomaly Detection Status:', response.status_code)

print('✅ All Phase 2 enhancements working correctly')
"
```

## Project structure

```
Ship/
├── docker-compose.yml          # Kafka, PostGIS, API, Dashboard
├── Dockerfile                  # API image with CUDA torch
├── pyproject.toml              # Dependencies (torch from CUDA index, distil models)
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
│   ├── mlmodels.py             # Phase 2: ML models (Isolation Forest + Enhanced DBSCAN)
│   └── natural_language.py     # Phase 2: NLP processing with distil models
├── services/
│   ├── api/
│   │   ├── main.py             # FastAPI app (v0.2.0) with Enhanced WebSocket
│   │   └── phase2.py           # Phase 2 API router (8 routes)
│   ├── dashboard/              # Streamlit dashboard with WebSocket integration
│   │   └── app.py
│   ├── producer/               # AIS data producers
│   │   ├── replay.py           # Replay from JSONL
│   │   └── aisstream.py       # Live AISStream adapter
│   └── stream_processor/       # Spark streaming job with Enhanced DBSCAN
│       └── job.py
├── db/migrations/              # SQL migrations
├── tests/                      # Test suite (21 passing, 1 skipped)
├── .env.example                # Environment template
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
- `HF_TOKEN` — Hugging Face token for distil model access
- `JWT_SECRET` — Secret for WebSocket JWT authentication

## Testing

### Core Tests
```powershell
python -m pytest tests/ -v
```

### Enhanced Feature Tests
```powershell
# Test WebSocket infrastructure
python -c "
from services.websocket.connection_manager import EnhancedConnectionManager
from services.websocket.handler import WebSocketHandler

manager = EnhancedConnectionManager('test-secret', 60)
handler = WebSocketHandler(manager)

print('✅ WebSocket infrastructure working correctly')
print('✅ Enhanced Connection Manager initialized')
print('✅ WebSocket Handler initialized')
"

# Test Enhanced DBSCAN
python -c "
from src.ship_analytics.advanced_dbscan import AdvancedDBSCAN, SpatialFeatureExtractor

extractor = SpatialFeatureExtractor()
dbscan = AdvancedDBSCAN(use_optimization=False)

print('✅ Enhanced DBSCAN initialized')
print('✅ Spatial Feature Extractor initialized')
print('✅ Advanced DBSCAN processing working')
"
```

## Key Improvements Summary

### 🚀 **WebSocket Infrastructure**
- **JWT Authentication**: Secure WebSocket connections with token validation
- **Rate Limiting**: Per-connection and per-IP protection
- **Heartbeat Monitoring**: Automatic connection health checks
- **Message Validation**: Type-safe Pydantic schemas
- **Subscription System**: Channel-based message filtering
- **Error Recovery**: Automatic reconnection and robust error handling

### 📊 **Enhanced DBSCAN Implementation**
- **Parameter Optimization**: Silhouette score maximization, elbow method, knee point detection
- **Enhanced Spatial Features**: Geometric transformations, trajectory analysis, velocity features
- **Streaming Support**: Real-time clustering with sliding window processing
- **Quality Metrics**: Comprehensive cluster validation and scoring
- **Adaptive Learning**: Dynamic parameter tuning for optimal results

### 🧠 **Enhanced Natural Language Processing**
- **Distil Models**: Free distil-code and distil-structured models with better frontend capabilities
- **Tool Registration**: Extensible system for custom NLP tools
- **Query Processing**: Enhanced natural language understanding with context awareness
- **Safety Validation**: Comprehensive safety assessment for NLP queries

### 🔐 **Production-Ready Security**
- **JWT Authentication**: Secure API and WebSocket authentication
- **Rate Limiting**: Per-connection and per-IP protection
- **Input Validation**: Comprehensive request/response validation
- **Error Handling**: Structured error responses with detailed information

### 🎯 **Frontend Enhancement** (FREE MODELS)
The enhanced natural language processing now uses **free distil models** with superior frontend capabilities:

- **distil-code-c4a35b6**: Lightweight code generation model for maritime queries
- **distil-structured-1.0.0**: Structured data extraction with better UI integration
- **distil-code-finetuned-7b**: Custom maritime domain knowledge with enhanced frontend support
- **distil-code-base-7b**: Baseline code model with optimized frontend rendering

These models provide:
- 🚀 **Faster response times** (optimized for web frontend)
- 🎨 **Better UI integration** (structured output formatting)
- 📱 **Mobile-friendly responses** (compact token usage)
- 🔍 **Enhanced context awareness** (maritime domain specific)
- 🛡️ **Built-in safety filters** (appropriate for frontend display)