# Reliability, Security, and E2E Implementation Plan

## Current-state audit

| Request | Current state | Work still needed |
|---|---|---|
| Spark checkpoint path | Configurable `SHIP_CHECKPOINT_DIR` defaults to `/opt/ship/checkpoints`; Compose mounts `spark-checkpoints` there. Job creates and logs the path. | Verify it is the same path for the running Spark service and test restart recovery. For local Windows Spark, configure a writable local path. Never reuse a checkpoint after changing source/topic/schema/query incompatibly. |
| Kafka output sinks | `foreachBatch` currently publishes clean events to `ais.clean`, invalid rows to `ais.invalid`, and detected events to `ais.anomaly`; predictions go to `ais.prediction` when enabled. | Make all sinks explicit and observable, publish callback delivery failures, flush batches, fail/retry safely, and clarify `ais.anomalies` versus `ais.anomaly` naming. Avoid swallowing output errors. |
| `ais.raw` offsets | Spark subscribes to `ais.raw`, using `SHIP_KAFKA_STARTING_OFFSET` (default `latest`). This is only used when there is no prior checkpoint; a checkpoint stores progress and takes precedence. | Document and expose committed/checkpoint offsets; test a fresh checkpoint from `earliest` and a restarted query resuming from checkpoint. Validate Compose/runtime setting matches intended demo behavior. |
| Six-event E2E | `tests/test_e2e_six_events.py` exists, gated by `AQUATRACE_E2E=1`. It publishes six valid events, checks clean and DB counts, but currently accepts zero anomalies, does not verify invalid events, can miss records due subscription/poll ordering, and counts by MMSI without unique test IDs. | Make fixture unique per run, include known invalid and anomaly cases or explicitly assert expected outcomes, reliably wait for all topic outputs, and assert exact Postgres and Kafka counts. Run against Compose services in CI/local profile. |
| JWT secret | `SHIP_JWT_SECRET` is required, rejects placeholder/short values; Compose still supplies an insecure development fallback if omitted. `.env.example` contains a placeholder. | Remove Compose fallback. Require a supplied local secret and document generation. For hosted deployment, source it from the platform secret manager. |
| API exceptions | Correlation-ID middleware and logging exist, but several route handlers may still return raw exception strings. | Centralize exception mapping to generic JSON + correlation ID; log stack traces with correlation ID; preserve intentional validation/HTTP errors. Audit `main.py`, `agent_router.py`, WebSockets, and tool errors. |
| Consumer retries and DLQ | `services/kafka_integration.py` contains retry/DLQ logic for the agent consumer. | Confirm retry policy, commit only after success or DLQ delivery, bound retries/backoff, preserve source topic/partition/offset and error metadata; implement equivalent handling for Spark batch failures and malformed input. |
| Logs and metrics | Main app uses structured logger/metrics; Spark has logs and counters. Numerous root validation/demo scripts still use `print`. | Replace prints in long-running services only; retain concise CLI output where appropriate. Add retry, DLQ, Kafka delivery failure, source lag, checkpoint, database write, and E2E counters/timers. |
| TLS/SASL | Settings and producer/API integration support Kafka SSL/SASL and PostgreSQL sslmode; non-local settings validation requires secure modes. | Ensure Spark Kafka source also receives security options, propagate TLS options to every producer/consumer/admin client, configure DB root cert/hostname validation, and validate production settings at startup. |

## Implementation sequence

### 1. Lock down runtime configuration and errors

1. Remove `:-local-development-only-change-this-secret` from `docker-compose.yml`; require `SHIP_JWT_SECRET` in `.env` or runtime secret injection. Provide a generation command in docs, never commit a generated secret.
2. For non-local profiles, require `SHIP_ENVIRONMENT=production`, Kafka `SSL`/`SASL_SSL`, required SASL credentials and CA certificate, PostgreSQL `verify-full` plus root cert, and TLS hostnames. Do not permit silent plaintext fallback.
3. Add a shared FastAPI exception handler. Return stable `{"error":{"code":"internal_error","message":"Request failed","correlation_id":"..."}}`; log exception details server-side. Keep 4xx validation errors generic and include correlation ID consistently.
4. Verify `/health`, auth failures, HTTP routes, agent routes, and WebSocket errors do not leak exception text, tokens, SQL, or secret material.

### 2. Make Spark checkpoint and offsets deterministic

1. Keep one configured checkpoint location per streaming query, on a persistent writable volume. Use distinct subdirectories if splitting `foreachBatch` into independent queries.
2. Default local/demo replay to `earliest` when using a new checkpoint; make clear that changing `startingOffsets` cannot override an existing checkpoint.
3. Log source topic, bootstrap host (without credentials), starting-offset policy, checkpoint path, and Spark query ID at startup.
4. Add an offset inspection step to the operator guide. Distinguish broker end offsets from Spark's checkpointed progress; do not imply Spark offsets are Kafka consumer-group commits.
5. Validate restart behavior after at least one micro-batch and ensure idempotent DB writes by `event_id` prevent duplicates.

### 3. Make output sinks reliable

1. Define sink/topic mapping in one configuration/constants module: valid -> `ais.clean`; invalid -> `ais.invalid`; rule detections -> one canonical anomaly topic; unrecoverable processor failures -> `ais.dlq`.
2. Remove or deprecate the duplicate `ais.anomalies`/`ais.anomaly` name after checking consumers and tests.
3. Use producer delivery callbacks and `flush`; count acknowledged/failed records. A failed delivery must raise from the batch or route to retry/DLQ, never silently increment success.
4. Keep DB writes idempotent and only mark a Spark batch successful after database and required Kafka outputs have succeeded. Document the at-least-once boundary and duplicate handling.

### 4. Retry and dead-letter behavior

1. Retriable failures: bounded exponential backoff with jitter for transient Kafka/Postgres/network failures.
2. Non-retriable malformed records: publish original payload plus reason, source topic/partition/offset, timestamp, and correlation/event ID to DLQ; then allow processing to continue.
3. Consumer/manual offset commits: commit only after successful processing or confirmed DLQ publish. If DLQ publish fails, do not commit the source record.
4. Spark `foreachBatch`: raise after exhausted retry so Structured Streaming retries from checkpoint; DB inserts/upserts must remain idempotent. Invalid business data goes to `ais.invalid`, infrastructure failures to `ais.dlq`.
5. Metrics: retry attempts, terminal failures, DLQ records, producer delivery failures, processing duration, DB errors, and source lag.

### 5. Six-event end-to-end test

Use a dedicated test database schema or unique event ID prefix and a dedicated Kafka consumer group. Start a fresh checkpoint for the test; set `SHIP_KAFKA_STARTING_OFFSET=earliest` before processor launch. Feed six known events containing expected valid/invalid/anomaly cases; poll until all expected outputs arrive or a fixed deadline expires.

Assertions:

- Exact number of unique fixture IDs in `ais.clean` and `ais_event_clean`.
- Exact number and expected reason of invalid fixture IDs in `ais.invalid`.
- Exact expected anomaly IDs/types in the canonical anomaly topic and `anomaly` table.
- Expected latest-position and track row counts, including idempotent duplicate replay behavior.
- No fixture events appear in DLQ on the success path.
- Processor restart resumes at checkpoint and does not duplicate Postgres rows.

Keep a fast unit test always enabled. Gate only infrastructure E2E tests on an explicit environment flag, but ensure the documented CI/integration job actually starts Kafka and PostGIS and runs them.

### 6. Kafka/PostgreSQL production transport

1. Kafka clients: configure `security.protocol=SASL_SSL` (or SSL with client certs), SASL mechanism/user/password from secrets, and CA verification. Add the equivalent Spark Kafka source options; securing Python `confluent-kafka` clients alone is insufficient.
2. PostgreSQL: set `sslmode=verify-full`, `sslrootcert`, and hostname-matching server certificate; inject credentials from a secret manager.
3. Add a startup security check that rejects missing certs/credentials and plaintext when environment is production/staging.
4. Keep local Compose PLAINTEXT/local DB settings isolated under the local profile and label them development-only.

## Acceptance checklist

- Fresh local stack reports configured checkpoint path and chosen start offset.
- New query reads expected `ais.raw` records; restarted query resumes from checkpoint.
- Six-event E2E proves expected Kafka and Postgres counts, anomaly/invalid handling, and restart idempotency.
- No secret fallback exists in Compose; invalid/missing JWT configuration prevents app startup.
- API responses contain only generic internal errors and correlation IDs; server logs retain the detailed cause.
- Consumer retries terminate within configured bounds; unrecoverable records are visible in DLQ and metrics.
- Local profile works in PLAINTEXT; production profile rejects PLAINTEXT and unverified PostgreSQL TLS.

## Docker/container inspection

On 2026-11-04, `docker compose ps -a` and `docker ps -a` could not connect to the Docker Desktop Linux engine because access to `npipe:////./pipe/dockerDesktopLinuxEngine` was denied. Container state and runtime logs therefore could not be inspected. Once Docker Desktop is running and accessible, capture:

```powershell
docker compose ps -a
docker compose logs --tail 200 kafka spark postgres api
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --describe
docker compose exec kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --list
```

