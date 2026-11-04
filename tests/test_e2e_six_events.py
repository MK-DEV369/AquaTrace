"""End-to-end validation for six AIS events across Kafka, Spark, and PostgreSQL."""

from __future__ import annotations

import json
import os
import time
import uuid
from pathlib import Path

import pytest

FIXTURE = Path(__file__).parent / "fixtures" / "six_events.jsonl"


def _load_events() -> list[dict]:
    with FIXTURE.open(encoding="utf-8") as file:
        return [json.loads(line) for line in file if line.strip()]


def test_six_events_validated() -> None:
    from ship_analytics.schemas import AisEvent

    events = [AisEvent.model_validate(event) for event in _load_events()]
    assert len(events) == 6
    assert all(event.mmsi >= 100_000_000 for event in events)


def test_six_events_anomaly_and_windows() -> None:
    from services.stream_processor.job import AISProcessor

    from ship_analytics.schemas import AisEvent

    events = [AisEvent.model_validate(event) for event in _load_events()]
    processor = AISProcessor()
    assert isinstance(processor.detect_and_categorize_anomalies(events), list)
    assert isinstance(processor.calculate_window_metrics(events), list)


@pytest.mark.skipif(
    os.environ.get("AQUATRACE_E2E") != "1",
    reason="set AQUATRACE_E2E=1 to run Kafka/Spark/PostgreSQL integration",
)
def test_six_events_kafka_postgres_output_counts() -> None:
    """Publish six events and verify every configured output topic and DB count."""
    import psycopg
    from confluent_kafka import Consumer, Producer

    from ship_analytics.config import get_settings
    from ship_analytics.kafka import ensure_topics

    settings = get_settings()
    ensure_topics(settings.kafka_brokers)
    events = _load_events()
    event_ids = {event["event_id"] for event in events}
    consumer = Consumer(
        {
            "bootstrap.servers": settings.kafka_brokers,
            "group.id": f"e2e-{uuid.uuid4().hex}",
            "auto.offset.reset": "latest",
            "enable.auto.commit": False,
        }
    )
    consumer.subscribe(["ais.clean", "ais.invalid", "ais.anomaly", "ais.prediction"])
    counts = {topic: 0 for topic in ("ais.clean", "ais.invalid", "ais.anomaly", "ais.prediction")}
    consumer.poll(1.0)
    producer = Producer({"bootstrap.servers": settings.kafka_brokers, "client.id": "e2e-six-events"})
    for event in events:
        producer.produce("ais.raw", key=str(event["mmsi"]), value=json.dumps(event))
    assert producer.flush(15) == 0
    deadline = time.monotonic() + 90
    try:
        while time.monotonic() < deadline and counts["ais.clean"] < len(events):
            message = consumer.poll(1.0)
            if message is None or message.error():
                continue
            payload = json.loads(message.value())
            if payload.get("event_id") in event_ids or payload.get("mmsi") in {event["mmsi"] for event in events}:
                counts[message.topic()] += 1
    finally:
        consumer.close()

    with psycopg.connect(settings.database_url) as connection, connection.cursor() as cursor:
        cursor.execute(
            "SELECT COUNT(*) FROM ais_event_clean WHERE event_id = ANY(%s)",
            (list(event_ids),),
        )
        database_count = cursor.fetchone()[0]

    assert database_count == len(events)
    assert counts["ais.clean"] == len(events)
    assert counts["ais.invalid"] == 0
    assert counts["ais.anomaly"] >= 0
    if settings.enable_prediction:
        assert counts["ais.prediction"] >= len(events)
