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
from ship_analytics.logging import configure_logging, get_logger
from ship_analytics.metrics import counter
from ship_analytics.schemas import AisEvent
from ship_analytics.vessel_types import normalize_vessel_type

AISSTREAM_URL = "wss://stream.aisstream.io/v0/stream"
logger = get_logger("producer.aisstream")


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
        vessel_type=normalize_vessel_type(metadata.get("ShipType"), int(report["UserID"])),
    )


def bbox_from_settings(value: str) -> tuple[float, float, float, float]:
    """Parse the environment bounding box as min_lat,min_lon,max_lat,max_lon."""
    try:
        min_lat, min_lon, max_lat, max_lon = (float(part.strip()) for part in value.split(","))
    except ValueError as exc:
        raise ValueError("SHIP_AISSTREAM_BBOX must contain four comma-separated coordinates") from exc
    if min_lat >= max_lat or min_lon >= max_lon:
        raise ValueError("AISStream bbox requires MIN_LAT < MAX_LAT and MIN_LON < MAX_LON")
    return min_lat, min_lon, max_lat, max_lon


async def stream(min_lat: float, min_lon: float, max_lat: float, max_lon: float) -> None:
    settings = get_settings()
    if not settings.aisstream_api_key:
        raise RuntimeError("SHIP_AISSTREAM_API_KEY is required for the live adapter")
    ensure_topics(settings.kafka_brokers)
    producer_config = {
        "bootstrap.servers": settings.kafka_brokers,
        "client.id": "aisstream",
        "security.protocol": settings.kafka_security_protocol,
    }
    if settings.kafka_security_protocol in {"SASL_PLAINTEXT", "SASL_SSL"}:
        producer_config.update(
            {
                "sasl.mechanism": settings.kafka_sasl_mechanism,
                "sasl.username": settings.kafka_sasl_username,
                "sasl.password": settings.kafka_sasl_password,
            }
        )
    if settings.kafka_security_protocol in {"SSL", "SASL_SSL"}:
        producer_config["ssl.ca.location"] = settings.kafka_ssl_ca_location
    producer = Producer(producer_config)
    subscription: dict[str, object] = {
        "APIKey": settings.aisstream_api_key,
        "BoundingBoxes": [[[min_lat, min_lon], [max_lat, max_lon]]],
        "FilterMessageTypes": ["PositionReport"],
    }
    if settings.aisstream_mmsi_filters:
        subscription["FiltersShipMMSI"] = [
            mmsi.strip() for mmsi in settings.aisstream_mmsi_filters.split(",") if mmsi.strip()
        ]

    while True:
        try:
            async with connect(AISSTREAM_URL, compression="deflate") as websocket:
                await websocket.send(json.dumps(subscription))
                logger.info(
                    "connected to AISStream bbox=%s,%s,%s,%s mmsi_filter_count=%d",
                    min_lat,
                    min_lon,
                    max_lat,
                    max_lon,
                    len(subscription.get("FiltersShipMMSI", [])),
                )
                async for raw_message in websocket:
                    try:
                        decoded = json.loads(raw_message)
                        if decoded.get("MessageType") == "SubscriptionConfirmation":
                            logger.info("AISStream subscription confirmed")
                            continue
                        event = normalize_position(decoded)
                        if event:
                            producer.produce("ais.raw", key=str(event.mmsi), value=event.model_dump_json())
                            producer.poll(0)
                            counter("aisstream_events_published_total")
                    except (KeyError, TypeError, ValueError) as exc:
                        counter("aisstream_invalid_messages_total")
                        logger.warning("ignored invalid AISStream payload: %s", exc)
        except Exception as exc:  # network interruptions and upstream reconnects are expected
            counter("aisstream_reconnects_total")
            logger.warning("AISStream connection lost: %s; retrying", exc)
            await asyncio.sleep(settings.aisstream_reconnect_seconds)


def main() -> None:
    settings = get_settings()
    configure_logging(settings.log_level)
    parser = argparse.ArgumentParser(description="Read a bounded AISStream position feed into Kafka.")
    parser.add_argument(
        "--bbox",
        nargs=4,
        type=float,
        metavar=("MIN_LAT", "MIN_LON", "MAX_LAT", "MAX_LON"),
        help="override SHIP_AISSTREAM_BBOX",
    )
    args = parser.parse_args()
    if args.bbox:
        min_lat, min_lon, max_lat, max_lon = args.bbox
        if min_lat >= max_lat or min_lon >= max_lon:
            parser.error("bbox requires MIN_LAT < MAX_LAT and MIN_LON < MAX_LON")
    else:
        try:
            min_lat, min_lon, max_lat, max_lon = bbox_from_settings(settings.aisstream_bbox)
        except ValueError as exc:
            parser.error(str(exc))
    asyncio.run(stream(min_lat, min_lon, max_lat, max_lon))


if __name__ == "__main__":
    main()

