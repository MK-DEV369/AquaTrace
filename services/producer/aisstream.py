"""Optional AISStream adapter. Replayed data remains the required demo source."""

import argparse
import asyncio
import json
from datetime import UTC, datetime
from uuid import uuid4

from confluent_kafka import Producer
from websockets.asyncio.client import connect

from ship_analytics.config import get_settings
from ship_analytics.kafka import ensure_topics
from ship_analytics.schemas import AisEvent

AISSTREAM_URL = "wss://stream.aisstream.io/v0/stream"


def normalize_position(message: dict[str, object]) -> AisEvent | None:
    """Map an AISStream PositionReport to the canonical contract; ignore other messages."""

    if message.get("MessageType") != "PositionReport":
        return None
    payload = message.get("Message")
    if not isinstance(payload, dict):
        return None
    report = payload.get("PositionReport")
    metadata = payload.get("MetaData")
    if not isinstance(report, dict):
        return None
    if not isinstance(metadata, dict):
        metadata = {}

    now = datetime.now(UTC)
    event_time = report.get("Timestamp") or now.isoformat()
    return AisEvent(
        event_id=f"aisstream:{report['UserID']}:{event_time}:{uuid4().hex[:8]}",
        source="aisstream",
        mmsi=int(report["UserID"]),
        event_time=event_time,
        ingested_at=now,
        latitude=float(report["Latitude"]),
        longitude=float(report["Longitude"]),
        sog_knots=float(report["Sog"]) if report.get("Sog") is not None else None,
        cog_degrees=float(report["Cog"]) if report.get("Cog") is not None else None,
        heading_degrees=int(report["TrueHeading"]) if report.get("TrueHeading") is not None else None,
        nav_status=str(report["NavigationalStatus"]) if report.get("NavigationalStatus") else None,
        vessel_type=str(metadata["ShipType"]) if metadata.get("ShipType") else None,
    )


async def stream(min_lat: float, min_lon: float, max_lat: float, max_lon: float) -> None:
    settings = get_settings()
    if not settings.aisstream_api_key:
        raise RuntimeError("SHIP_AISSTREAM_API_KEY is required for the live adapter")
    ensure_topics(settings.kafka_brokers)
    producer = Producer({"bootstrap.servers": settings.kafka_brokers, "client.id": "aisstream"})
    subscription = {
        "APIKey": settings.aisstream_api_key,
        "BoundingBoxes": [[[min_lat, min_lon], [max_lat, max_lon]]],
        "FilterMessageTypes": ["PositionReport"],
    }
    async with connect(AISSTREAM_URL) as websocket:
        await websocket.send(json.dumps(subscription))
        async for raw_message in websocket:
            try:
                event = normalize_position(json.loads(raw_message))
                if event:
                    producer.produce("ais.raw", key=str(event.mmsi), value=event.model_dump_json())
                    producer.poll(0)
            except (KeyError, TypeError, ValueError):
                # Bad external payloads are ignored here; production code should publish a source-error metric.
                continue


def main() -> None:
    parser = argparse.ArgumentParser(description="Read a bounded AISStream position feed into Kafka.")
    parser.add_argument("--bbox", nargs=4, type=float, metavar=("MIN_LAT", "MIN_LON", "MAX_LAT", "MAX_LON"), required=True)
    args = parser.parse_args()
    min_lat, min_lon, max_lat, max_lon = args.bbox
    if min_lat >= max_lat or min_lon >= max_lon:
        parser.error("bbox requires MIN_LAT < MAX_LAT and MIN_LON < MAX_LON")
    asyncio.run(stream(min_lat, min_lon, max_lat, max_lon))


if __name__ == "__main__":
    main()

