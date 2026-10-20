import argparse
import time
from collections.abc import Iterator
from pathlib import Path

from confluent_kafka import Producer

from ship_analytics.config import get_settings
from ship_analytics.kafka import ensure_topics
from ship_analytics.schemas import AisEvent


def events_from_jsonl(path: Path) -> Iterator[AisEvent]:
    for line_number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1):
        if not line.strip():
            continue
        try:
            yield AisEvent.model_validate_json(line)
        except Exception as exc:
            raise ValueError(f"invalid event at {path}:{line_number}") from exc


def publish(path: Path, rate_per_second: float, loop: bool) -> None:
    settings = get_settings()
    ensure_topics(settings.kafka_brokers)
    producer = Producer({"bootstrap.servers": settings.kafka_brokers, "client.id": "ais-replay"})
    delay = 1 / rate_per_second

    while True:
        count = 0
        for event in events_from_jsonl(path):
            producer.produce(
                "ais.raw",
                key=str(event.mmsi),
                value=event.model_dump_json(),
            )
            producer.poll(0)
            count += 1
            time.sleep(delay)
        producer.flush(10)
        print(f"published {count} event(s) from {path}")
        if not loop:
            return


def main() -> None:
    parser = argparse.ArgumentParser(description="Replay canonical AIS JSONL events to Kafka.")
    parser.add_argument("--file", type=Path, required=True)
    parser.add_argument("--rate", type=float, default=5.0, help="events per second")
    parser.add_argument("--loop", action="store_true")
    args = parser.parse_args()
    if args.rate <= 0:
        parser.error("--rate must be greater than zero")
    publish(args.file, args.rate, args.loop)


if __name__ == "__main__":
    main()

