"""High-volume replay of a local AISDK CSV without loading the full file in memory."""

from __future__ import annotations

import argparse
import csv
import time
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

from confluent_kafka import Producer

from ship_analytics.config import get_settings
from ship_analytics.kafka import ensure_topics
from ship_analytics.logging import configure_logging, get_logger
from ship_analytics.metrics import counter
from ship_analytics.schemas import AisEvent
from ship_analytics.vessel_types import normalize_vessel_type

logger = get_logger("producer.csv_replay")


def _number(value: str | None) -> float | None:
    if value is None or not value.strip():
        return None
    return float(value)


def events_from_csv(path: Path, vessel_limit: int, preserve_event_time: bool = False):
    """Yield the first valid event for each vessel, up to ``vessel_limit`` vessels."""
    seen_mmsi: set[int] = set()
    run_id = uuid4().hex
    with path.open("r", encoding="utf-8-sig", newline="") as file:
        reader = csv.DictReader(file)
        for row_number, row in enumerate(reader, start=2):
            try:
                mmsi = int(row["MMSI"])
                latitude = float(row["Latitude"])
                longitude = float(row["Longitude"])
                if not 100_000_000 <= mmsi <= 999_999_999 or mmsi in seen_mmsi:
                    continue
                source_time = datetime.strptime(row["# Timestamp"], "%d/%m/%Y %H:%M:%S").replace(tzinfo=UTC)
                event_time = source_time if preserve_event_time else datetime.now(UTC)
                seen_mmsi.add(mmsi)
                yield AisEvent(
                    event_id=f"aisdk:{run_id}:{mmsi}:{source_time.isoformat()}:{row_number}",
                    source="replay",
                    mmsi=mmsi,
                    event_time=event_time,
                    ingested_at=datetime.now(UTC),
                    latitude=latitude,
                    longitude=longitude,
                    sog_knots=_number(row.get("SOG")),
                    cog_degrees=_number(row.get("COG")),
                    heading_degrees=int(row["Heading"]) if (row.get("Heading") or "").isdigit() else None,
                    nav_status=row.get("Navigational status") or None,
                    vessel_type=normalize_vessel_type(row.get("Ship type"), mmsi),
                )
                if len(seen_mmsi) >= vessel_limit:
                    return
            except (KeyError, TypeError, ValueError):
                counter("aisdk_invalid_rows_total")


def publish(path: Path, vessel_limit: int, rate_per_second: float, preserve_event_time: bool = False) -> None:
    settings = get_settings()
    ensure_topics(settings.kafka_brokers)
    config = {
        "bootstrap.servers": settings.kafka_brokers,
        "client.id": "aisdk-csv-replay",
        "security.protocol": settings.kafka_security_protocol,
    }
    if settings.kafka_security_protocol in {"SASL_PLAINTEXT", "SASL_SSL"}:
        config.update(
            {
                "sasl.mechanism": settings.kafka_sasl_mechanism,
                "sasl.username": settings.kafka_sasl_username,
                "sasl.password": settings.kafka_sasl_password,
            }
        )
    if settings.kafka_security_protocol in {"SSL", "SASL_SSL"}:
        config["ssl.ca.location"] = settings.kafka_ssl_ca_location
    producer = Producer(config)
    count = 0
    for event in events_from_csv(path, vessel_limit, preserve_event_time):
        producer.produce("ais.raw", key=str(event.mmsi), value=event.model_dump_json())
        producer.poll(0)
        count += 1
        counter("aisdk_events_published_total")
        time.sleep(1 / rate_per_second)
    if producer.flush(30):
        raise RuntimeError("timed out publishing AISDK replay events")
    if count < vessel_limit:
        logger.warning(
            "dataset contains %d valid unique vessels, below requested target of %d",
            count,
            vessel_limit,
        )
    else:
        logger.info("published %d unique vessels from %s", count, path)


def main() -> None:
    configure_logging(get_settings().log_level)
    parser = argparse.ArgumentParser(description="Replay a bounded number of unique vessels from an AISDK CSV.")
    parser.add_argument("--file", type=Path, required=True)
    parser.add_argument("--vessels", type=int, default=1_000)
    parser.add_argument("--rate", type=float, default=200.0)
    parser.add_argument(
        "--preserve-event-time",
        action="store_true",
        help="keep historical CSV timestamps instead of using current time for a live dashboard bootstrap",
    )
    args = parser.parse_args()
    if args.vessels < 1 or args.rate <= 0:
        parser.error("--vessels and --rate must be greater than zero")
    publish(args.file, args.vessels, args.rate, args.preserve_event_time)


if __name__ == "__main__":
    main()
