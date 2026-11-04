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


def _events_for_run(run_id: str) -> list[dict]:
    """Make every infrastructure run distinguishable from prior replay data."""
    events = []
    for index, event in enumerate(_load_events()):
        copy = dict(event)
        copy["event_id"] = f"e2e:{run_id}:{index}"
        events.append(copy)
    return events


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
    run_id = uuid.uuid4().hex
    events = _events_for_run(run_id)
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
    seen_ids = {topic: set() for topic in ("ais.clean", "ais.invalid", "ais.anomaly", "ais.prediction")}
    # Join the group before producing so the 'latest' test subscription cannot miss output records.
    consumer.poll(1.0)
    producer = Producer({"bootstrap.servers": settings.kafka_brokers, "client.id": "e2e-six-events"})
    for event in events:
        producer.produce("ais.raw", key=str(event["mmsi"]), value=json.dumps(event))
    assert producer.flush(15) == 0
    deadline = time.monotonic() + 90
    try:
        while time.monotonic() < deadline and len(seen_ids["ais.clean"]) < len(events):
            message = consumer.poll(1.0)
            if message is None or message.error():
                continue
            payload = json.loads(message.value())
            if payload.get("event_id") in event_ids:
                seen_ids[message.topic()].add(payload["event_id"])
    finally:
        consumer.close()

    with psycopg.connect(settings.database_url) as connection, connection.cursor() as cursor:
        cursor.execute(
            "SELECT COUNT(*) FROM ais_event_clean WHERE event_id = ANY(%s)",
            (list(event_ids),),
        )
        clean_database_count = cursor.fetchone()[0]
        cursor.execute(
            "SELECT COUNT(*) FROM vessel_track_point WHERE event_id = ANY(%s)",
            (list(event_ids),),
        )
        track_database_count = cursor.fetchone()[0]
        cursor.execute(
            "SELECT COUNT(*) FROM vessel_latest WHERE mmsi = ANY(%s)",
            ([event["mmsi"] for event in events],),
        )
        latest_database_count = cursor.fetchone()[0]

    assert clean_database_count == len(events)
    assert track_database_count == len(events)
    assert latest_database_count == len(events)
    assert seen_ids["ais.clean"] == event_ids
    assert seen_ids["ais.invalid"] == set()
    assert seen_ids["ais.anomaly"] == set()
    if settings.enable_prediction:
        assert len(seen_ids["ais.prediction"]) == len(events)
