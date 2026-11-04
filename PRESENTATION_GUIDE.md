# AquaTrace presentation and quickstart guide

## Quick start (Docker Compose)

Prerequisites: Docker Desktop running, Python 3.12, Java 17 only if running Spark outside Docker.

From PowerShell in the repository:

```powershell
Copy-Item .env.example .env   # if .env does not exist yet
docker compose up --build -d
docker compose ps
docker compose logs -f kafka spark
```

In a second terminal, start replaying the checked-in sample AIS data:

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install -e .
python -m services.producer.replay --file data/sample/ais_events.jsonl --rate 2 --loop
```

Open `http://localhost:8501` for the dashboard, `http://localhost:8000/docs` for the REST API, and `http://localhost:8000/force-graph/` for the GPU-rendered 3D relationship graph. The graph polls the vessel API every three seconds; Three.js/WebGL renders nodes and links, while `3d-force-graph`/`d3-force-3d` computes the force layout in the browser.

Useful checks:

```powershell
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic ais.raw
docker compose exec kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --list
docker compose logs --tail 100 spark
Invoke-RestMethod http://localhost:8000/health
Invoke-RestMethod 'http://localhost:8000/vessels?limit=10'
```

Stop services with `docker compose down`. The named `postgres-data` and `spark-checkpoints` volumes persist across that command. `docker compose down -v` removes volumes and is destructive: it deletes local database and checkpoint data.

## Presentation walkthrough

### 1. Producer and consumer

- **Producer:** `services/producer/replay.py`. It reads JSON Lines, validates each record using `AisEvent`, and sends JSON to `ais.raw` with MMSI as the Kafka key. `services/producer/aisstream.py` is the optional live WebSocket source.
- **Stream consumer/processor:** `services/stream_processor/job.py` uses Spark Structured Streaming's Kafka source, subscribes to `ais.raw`, validates and deduplicates events, then writes clean, invalid, anomaly, and prediction outputs and persists vessel state to PostGIS.
- **Serving consumer:** API reads the persisted tables. The graph/dashboard read API endpoints; they do not consume Kafka directly.

Explain the role split: producers publish events; Kafka stores ordered partition logs; consumers read at their own pace using offsets; Spark performs event-time processing and checkpointed recovery.

### 2. Partitions and local storage

`src/ship_analytics/kafka.py` creates **10 application topics, each with 3 partitions and replication factor 1**. MMSI is the record key, so Kafka's key partitioner sends a vessel's events consistently to one partition. Three partitions permit up to three parallel consumers in a consumer group for that topic; one local broker cannot provide broker-level redundancy.

Kafka broker logs are stored in the broker container's writable filesystem because Compose currently does not mount a Kafka data volume. Thus Kafka records do **not** survive container recreation. PostgreSQL data is in the named `postgres-data` volume; Spark checkpoints are in `spark-checkpoints`. For a durable local Kafka demonstration, add a named volume mounted at `/var/lib/kafka/data` and set Kafka `log.dirs` to that path before relying on broker data across container recreation.

PostGIS stores the queryable clean event history, latest vessel state, track points, and anomalies. Kafka is the streaming log; PostGIS is the serving/query store; Spark checkpoints track processing progress.

### 3. Topics and how they connect

| Topic | Role | Current code path |
|---|---|---|
| `ais.raw` | Input AIS events | Replay/live producer writes; Spark consumes. |
| `ais.clean` | Normalized valid events | Spark emits; downstream inspection/replay. |
| `ais.invalid` | Rejected payloads | Spark emits with validation reason. |
| `ais.anomaly` | Current job's anomaly sink | Spark job emits detected anomaly events here. |
| `ais.prediction` | Predicted vessel positions | Used when prediction processing is enabled and model artifacts are available. |
| `ais.alert` | Alert stream | Provisioned for alert integrations. |
| `ais.dlq` | Dead-letter/rejected processing events | Provisioned for failure handling. |
| `agent.investigations` | Agent investigation results | Agent/Kafka integration. |
| `agent.alerts` | Agent-generated alerts | Agent/Kafka integration. |

Topics are independent logs connected by applications, not direct topic-to-topic wires. Producers write to topics; Spark reads input and produces output records; API reads PostGIS. Verify actual message counts live because topic creation does not mean every topic currently has records.

### 4. Cluster metadata and partitions

Use `kafka-topics.sh --describe` to show partition IDs, leaders, replicas, and in-sync replicas (ISR). Use `kafka-topics.sh --list` for topic metadata and `kafka-consumer-groups.sh --describe --group <group>` for consumer offsets/lag. This Compose setup is a **single-node KRaft development cluster**: one broker/controller node, one replica per topic partition, so ISR size is normally 1. It demonstrates Kafka metadata and partitions, not multi-node fault tolerance.

### 5. Kafka configuration

The `kafka` service in `docker-compose.yml` sets KRaft roles, node ID, listeners, advertised listeners, listener protocol mapping, controller quorum, and single-node replication/transaction defaults. Container clients connect to `kafka:9092`; host tools connect to `localhost:29092`. Port 9093 is the internal controller listener. The project sets bootstrap address via `SHIP_KAFKA_BROKERS`; topic count/partition defaults are in `src/ship_analytics/kafka.py`.

### 6. Dockerized Kafka source

Show `docker-compose.yml` under the `kafka:` service. It uses the official `apache/kafka:4.2.0` image in KRaft combined broker/controller mode. There is no ZooKeeper. Point to the environment variables in the previous section and note that the Compose file currently has no Kafka volume mount, so broker log persistence must be added if expected in the demo.

### 7. Elasticsearch

Elasticsearch is **not configured or used** in this repository's current Compose stack. Do not present Elasticsearch queries as implemented project behavior. The equivalent current queries are SQL/PostGIS, e.g.:

```sql
SELECT mmsi, event_time, ST_Y(geom) AS latitude, ST_X(geom) AS longitude
FROM vessel_latest ORDER BY event_time DESC LIMIT 20;

SELECT event_id, mmsi, event_time, sog_knots
FROM ais_event_clean WHERE mmsi = 563001001 ORDER BY event_time;

SELECT anomaly_type, severity, COUNT(*)
FROM anomaly GROUP BY anomaly_type, severity ORDER BY COUNT(*) DESC;
```

### 8. Logs, metrics, and visualization

- `docker compose logs -f producer` only works if producer is containerized; in this quickstart it runs in a terminal, so show that terminal's output.
- `docker compose logs -f spark` shows processing and Kafka sink logs; `docker compose logs -f kafka` shows broker startup and request logs.
- `GET /metrics` exposes Prometheus-format application metrics; `GET /health` checks API/database health.
- Dashboard at port 8501 shows vessel map, fleet metrics, anomalies, tracks, and query assistant.
- 3D graph at `/force-graph/` uses WebGL through Three.js. Nodes represent vessels, and links are derived from current coordinates within 5 km. The force layout is client-side; AIS data arrives via the API from PostGIS after the Kafka/Spark pipeline.

## Short theory answers

- **Why Kafka?** It decouples AIS producers from processors, buffers bursts, retains events for replay, and partitions a keyed event log for parallel consumption.
- **What is an offset?** A consumer's position in one partition log. Spark checkpoints its progress so a restart can resume processing.
- **What is a consumer group?** Consumers sharing a group divide assigned partitions; one partition is read by one consumer in that group at a time.
- **Why key by MMSI?** Events for one vessel consistently map to one partition, preserving that vessel's partition order and enabling per-vessel state.
- **Why three partitions?** A small local parallelism demonstration; it is a configurable starter value, not a measured throughput optimum.
- **Why PostGIS as well as Kafka?** Kafka is the append-oriented event log. PostGIS supports indexed spatial queries and API/dashboard reads over current and historical vessel positions.
- **What does GPU mean here?** Three.js/WebGL draws the 3D scene on the GPU. The force-layout physics in `3d-force-graph` runs in browser JavaScript; this implementation does not claim GPU compute for the force calculation.
- **Limitations?** Single broker, replication factor 1, local development credentials, bounded demo data, no Elasticsearch, and no production availability guarantees.

