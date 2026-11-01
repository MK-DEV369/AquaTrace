# AquaTrace Security and Kafka E2E Review

Date: 2026-11-01

## Scope

This basic review covered the FastAPI API, agent API, WebSocket handlers, Docker
Compose exposure, authentication configuration, Kafka producer, Spark streaming
job, and the sample replay path. It was not a penetration test.

## Remediated in this change

1. Added bearer-token protection to non-health HTTP API routes. The token is
   supplied through `SHIP_API_TOKEN`; requests without a valid token receive
   `401`.
2. Removed the unauthenticated `/ws/updates-legacy` broadcast endpoint.
3. Changed enhanced WebSocket authentication to reject missing or invalid
   tokens instead of creating anonymous read sessions.
4. Protected privileged WebSocket handlers through the now-required
   authenticated connection path.
5. Bound PostgreSQL, Kafka, and API host ports to `127.0.0.1` in the local
   Compose configuration. Production deployments still require TLS and
   service authentication.

The Compose development token is intentionally a placeholder:
`dev-api-token-change-me`. Replace it through a secret mechanism before sharing
or deploying the stack.

## Remaining security findings

| ID | Severity | Location | Finding | Recommended action |
|---|---|---|---|---|
| SEC-06 | HIGH | `services/auth/zero_trust.py` global manager initialization | A known fallback JWT secret is used when no secret is configured. | Require `SHIP_JWT_SECRET` from a secret manager and fail startup for missing or known development values. |
| SEC-07 | MEDIUM | `services/api/agent_router.py` error handlers | Several endpoints return `str(e)` to clients, which can disclose database or implementation details. | Log the exception server-side and return a generic message with a correlation ID. |
| SEC-08 | MEDIUM | `docker-compose.yml` Kafka listeners and PostgreSQL development credentials | Local Compose still uses plaintext transport and development credentials inside the Docker network. | Configure SASL/TLS for shared environments and use generated secrets. |
| SEC-09 | MEDIUM | `services/kafka_integration.py` consumer loop | Errors are printed and processing continues without a dead-letter topic, retry policy, or durable failure record. | Add bounded retries, a dead-letter topic, structured logs, and commit offsets only after successful processing. |
| SEC-10 | MEDIUM | `services/kafka_integration.py` `store_event_for_analysis` | The method is a placeholder and silently drops consumed events. | Persist the event or remove the claim that the consumer stores events. |
| SEC-11 | LOW | `services/kafka_integration.py` `publish_investigation_result` and `publish_investigation_alert` | Producer delivery callbacks are not checked and flush errors are not surfaced. | Add delivery callbacks, bounded flush handling, and explicit failure metrics. |

## Kafka end-to-end findings

### Verified

- Kafka and PostgreSQL containers started healthy.
- The replay producer connected to `localhost:29092`.
- Six sample AIS events were published to `ais.raw`.
- `ais.clean`, `ais.invalid`, and `ais.anomalies` were created by
  `ensure_topics`.
- Six messages were consumed successfully from `ais.raw`.
- The API returned `200 {"status":"ok"}` over IPv4.
- Phase 1 tests passed: `19 passed, 1 skipped`.
- Schema tests passed: `2 passed`.

### Not complete

- The Spark job started but did not persist the replayed events:
  `ais_event_clean=0`, `vessel_latest=0`, and `vessel_track_point=0`.
- The job only consumes `ais.raw` and writes Postgres. It does not publish to
  the documented `ais.clean`, `ais.invalid`, or `ais.anomalies` topics.
- The streaming job uses the checkpoint path `checkpoints`, while Compose
  mounts `/opt/ship/checkpoints`; this makes checkpoint persistence dependent
  on the process working directory.
- The Spark callback executes database writes in the driver and materializes
  each batch with `toLocalIterator`; this is not suitable for sustained
  production throughput.
- There is no explicit end-to-end assertion connecting a replay event to a
  database row and an anomaly-topic message.

## Recommended pending work

1. Fix the Spark callback and checkpoint path, then add a deterministic
   six-event E2E test.
2. Publish clean, invalid, and anomaly records from the Spark pipeline.
3. Add dead-letter and retry handling for malformed Kafka messages.
4. Replace development secrets and plaintext internal transport for deployment.
5. Add API/WebSocket authentication integration tests, including negative cases.
6. Replace print-based diagnostics with structured logging and metrics.
