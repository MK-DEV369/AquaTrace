"""Stream processor job for Phase 1: validate, deduplicate, publish, and persist AIS events."""

import json
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import psycopg
from confluent_kafka import Producer
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, from_json, lit
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from ship_analytics.anomalies import AnomalyDetector
from ship_analytics.config import get_settings
from ship_analytics.logging import get_logger
from ship_analytics.metrics import counter, histogram
from ship_analytics.schemas import AisEvent
from ship_analytics.trajectory_models import TrajectoryPredictor
from ship_analytics.windows import WindowCalculator

logger = get_logger("stream_processor")

EVENT_SCHEMA = StructType(
    [
        StructField("event_id", StringType(), False),
        StructField("source", StringType(), False),
        StructField("mmsi", IntegerType(), False),
        StructField("event_time", TimestampType(), False),
        StructField("ingested_at", TimestampType(), False),
        StructField("latitude", DoubleType(), False),
        StructField("longitude", DoubleType(), False),
        StructField("sog_knots", DoubleType(), True),
        StructField("cog_degrees", DoubleType(), True),
        StructField("heading_degrees", IntegerType(), True),
        StructField("nav_status", StringType(), True),
        StructField("vessel_type", StringType(), True),
    ]
)


class KafkaSinkError(RuntimeError):
    """Raised after a required Kafka output cannot be delivered or dead-lettered."""


def _as_utc(value: datetime) -> datetime:
    """Treat Spark's timezone-naive timestamps as UTC."""
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


class AISProcessor:
    """Main processing pipeline for AIS events."""

    def __init__(self):
        self.detector = AnomalyDetector()
        self.window_calculator = WindowCalculator(window_minutes=60)
        self.processed_events = []
        self.anomalies = []
        self.vessel_states = {}
        self.settings = get_settings()
        producer_config = {
            "bootstrap.servers": self.settings.kafka_brokers,
            "client.id": "spark-sink",
            "security.protocol": self.settings.kafka_security_protocol,
        }
        if self.settings.kafka_security_protocol in {"SASL_PLAINTEXT", "SASL_SSL"}:
            producer_config.update(
                {
                    "sasl.mechanism": self.settings.kafka_sasl_mechanism,
                    "sasl.username": self.settings.kafka_sasl_username,
                    "sasl.password": self.settings.kafka_sasl_password,
                }
            )
        if self.settings.kafka_security_protocol in {"SSL", "SASL_SSL"}:
            producer_config["ssl.ca.location"] = self.settings.kafka_ssl_ca_location
        self._producer = Producer(producer_config)
        self._predictors = (
            {
                model_type: TrajectoryPredictor(model_type=model_type, horizon_minutes=60)
                for model_type in ("lstm", "transformer")
            }
            if self.settings.enable_prediction
            else {}
        )

    def _publish(self, topic: str, key: str | None, value: dict[str, Any]) -> None:
        """Publish a required output, retrying before failing the Spark batch."""
        payload = json.dumps(value, default=str)
        last_error: Exception | None = None
        for attempt in range(1, self.settings.kafka_max_retries + 1):
            delivery_error: list[Exception] = []

            def delivered(error, _message) -> None:
                if error is not None:
                    delivery_error.append(error)

            try:
                self._producer.produce(topic, key=key, value=payload, on_delivery=delivered)
                pending = self._producer.flush(10)
                if pending == 0 and not delivery_error:
                    counter("kafka_sink_messages_total", labels={"topic": topic})
                    return
                last_error = KafkaSinkError(
                    f"Kafka delivery did not complete for topic {topic} (pending={pending})"
                )
            except Exception as exc:  # pragma: no cover - broker/network dependent
                last_error = exc

            counter("kafka_sink_retry_total", labels={"topic": topic})
            logger.warning("kafka sink retry topic=%s attempt=%d", topic, attempt)
            time.sleep(min(2 ** (attempt - 1), 8))

        if topic != self.settings.kafka_dlq_topic:
            self._publish_dlq(topic, key, payload, last_error)
        counter("kafka_sink_failure_total", labels={"topic": topic})
        raise KafkaSinkError(f"required Kafka output failed for topic {topic}") from last_error

    def _publish_dlq(
        self, source_topic: str, key: str | None, payload: str, error: Exception | None
    ) -> None:
        """Persist an unrecoverable output failure with enough context to replay it."""
        envelope = {
            "source_topic": source_topic,
            "key": key,
            "payload": payload,
            "error_type": type(error).__name__ if error else "UnknownError",
            "failed_at": datetime.now(UTC).isoformat(),
        }
        delivery_error: list[Exception] = []

        def delivered(delivery_failure, _message) -> None:
            if delivery_failure is not None:
                delivery_error.append(delivery_failure)

        self._producer.produce(
            self.settings.kafka_dlq_topic,
            key=key,
            value=json.dumps(envelope),
            on_delivery=delivered,
        )
        pending = self._producer.flush(10)
        if pending or delivery_error:
            raise KafkaSinkError("dead-letter publish failed")
        counter("kafka_messages_dlq_total", labels={"topic": source_topic})

    def validate_events(self, raw: DataFrame) -> tuple[DataFrame, DataFrame]:
        """Parse and validate AIS events from Kafka."""
        parsed = raw.select(from_json(col("value").cast("string"), EVENT_SCHEMA).alias("event"))
        events = parsed.select("event.*")

        condition = (
            col("event_id").isNotNull()
            & col("mmsi").between(100_000_000, 999_999_999)
            & col("latitude").between(-90, 90)
            & col("longitude").between(-180, 180)
            & (col("sog_knots").isNull() | col("sog_knots").between(0, 102.2))
        )

        invalid = events.where(~condition).withColumn("validation_error", lit("schema or range validation failed"))
        clean = events.where(condition).withWatermark("event_time", "10 minutes").dropDuplicates(["event_id"])

        return clean, invalid

    def detect_and_categorize_anomalies(self, events: list[AisEvent]) -> list:
        """Detect anomalies in events and categorize them."""
        batch_anomalies = []
        for event in events:
            previous_event = self.vessel_states.get(event.mmsi)
            last_known_state = self.vessel_states.get(event.mmsi)

            anomalies = self.detector.detect_anomalies(
                event, previous_event, last_known_state
            )
            self.anomalies.extend(anomalies)
            batch_anomalies.extend(anomalies)

            self.vessel_states[event.mmsi] = event

        return batch_anomalies

    def calculate_window_metrics(self, events: list[AisEvent]) -> list:
        """Calculate tumbling window metrics from events."""
        return self.window_calculator.tumbling_windows(events, self.anomalies)

    def upsert_to_postgis(self, batch: DataFrame, _: int) -> None:
        """Batch insert/update events to PostGIS."""
        events = []
        for row in batch.select(*EVENT_SCHEMA.fieldNames()).toLocalIterator():
            events.append({
                "event_id": row[0],
                "source": row[1],
                "mmsi": row[2],
                "event_time": row[3],
                "ingested_at": row[4],
                "latitude": row[5],
                "longitude": row[6],
                "sog_knots": row[7],
                "cog_degrees": row[8],
                "heading_degrees": row[9],
                "nav_status": row[10],
                "vessel_type": row[11],
            })

        self.processed_events.extend(events)

        settings = self.settings
        with psycopg.connect(settings.database_url) as conn, conn.cursor() as cursor:
            for event in events:
                ais_event = AisEvent(
                    event_id=event["event_id"],
                    source=event["source"],
                    mmsi=event["mmsi"],
                    event_time=_as_utc(event["event_time"]),
                    ingested_at=_as_utc(event["ingested_at"]),
                    latitude=event["latitude"],
                    longitude=event["longitude"],
                    sog_knots=event["sog_knots"],
                    cog_degrees=event["cog_degrees"],
                    heading_degrees=event["heading_degrees"],
                    nav_status=event["nav_status"],
                    vessel_type=event["vessel_type"],
                )

                cursor.execute(
                    """
                    INSERT INTO ais_event_clean (
                        event_id, source, mmsi, event_time, ingested_at,
                        geom, sog_knots, cog_degrees, heading_degrees,
                        nav_status, vessel_type
                    ) VALUES (%s, %s, %s, %s, %s, ST_SetSRID(ST_MakePoint(%s, %s), 4326), %s, %s, %s, %s, %s)
                    ON CONFLICT (event_id) DO NOTHING
                    """,
                    (
                        ais_event.event_id,
                        ais_event.source,
                        ais_event.mmsi,
                        ais_event.event_time,
                        ais_event.ingested_at,
                        ais_event.longitude,
                        ais_event.latitude,
                        ais_event.sog_knots,
                        ais_event.cog_degrees,
                        ais_event.heading_degrees,
                        ais_event.nav_status,
                        ais_event.vessel_type,
                    ),
                )
                cursor.execute(
                    """
                    INSERT INTO vessel_latest (
                        mmsi, event_time, geom, sog_knots, cog_degrees,
                        vessel_type, updated_at
                    ) VALUES (%s, %s, ST_SetSRID(ST_MakePoint(%s, %s), 4326), %s, %s, %s, now())
                    ON CONFLICT (mmsi) DO UPDATE SET
                        event_time = EXCLUDED.event_time, geom = EXCLUDED.geom,
                        sog_knots = EXCLUDED.sog_knots, cog_degrees = EXCLUDED.cog_degrees,
                        vessel_type = EXCLUDED.vessel_type, updated_at = now()
                    WHERE EXCLUDED.event_time > vessel_latest.event_time
                    """,
                    (
                        ais_event.mmsi,
                        ais_event.event_time,
                        ais_event.longitude,
                        ais_event.latitude,
                        ais_event.sog_knots,
                        ais_event.cog_degrees,
                        ais_event.vessel_type,
                    ),
                )
                cursor.execute(
                    """
                    INSERT INTO vessel_track_point (
                        event_id, mmsi, event_time, geom, sog_knots, cog_degrees
                    ) VALUES (%s, %s, %s, ST_SetSRID(ST_MakePoint(%s, %s), 4326), %s, %s)
                    ON CONFLICT (event_id) DO NOTHING
                    """,
                    (
                        ais_event.event_id,
                        ais_event.mmsi,
                        ais_event.event_time,
                        ais_event.longitude,
                        ais_event.latitude,
                        ais_event.sog_knots,
                        ais_event.cog_degrees,
                    ),
                )

    def persist_anomalies(self, anomalies: list) -> None:
        """Store the current micro-batch's explainable anomalies for API/dashboard reads."""
        if not anomalies:
            return
        with psycopg.connect(self.settings.database_url) as conn, conn.cursor() as cursor:
            for anomaly in anomalies:
                cursor.execute(
                    """
                    INSERT INTO anomaly (
                        mmsi, event_time, anomaly_type, severity, explanation, geom, evidence_json
                    ) VALUES (%s, %s, %s, %s, %s, ST_SetSRID(ST_MakePoint(%s, %s), 4326), %s::jsonb)
                    """,
                    (
                        anomaly.mmsi,
                        anomaly.event_time,
                        anomaly.anomaly_type,
                        anomaly.severity,
                        anomaly.explanation,
                        anomaly.geom.x,
                        anomaly.geom.y,
                        json.dumps(anomaly.evidence_json),
                    ),
                )


    def process_event_batch(self, batch_df: DataFrame, _: int) -> None:
        """Process a batch of events through the full pipeline."""
        t0 = datetime.now(UTC)
        clean_df, invalid_df = self.validate_events(batch_df)

        # Invalid payloads are a business-data sink, distinct from the infrastructure DLQ.
        for row in invalid_df.toLocalIterator():
            self._publish(
                "ais.invalid",
                None,
                {"event": row.asDict() if hasattr(row, "asDict") else str(row)},
            )

        events = []
        for row in clean_df.select(*EVENT_SCHEMA.fieldNames()).toLocalIterator():
            event = AisEvent(
                event_id=row[0],
                source=row[1],
                mmsi=row[2],
                event_time=_as_utc(row[3]),
                ingested_at=_as_utc(row[4]),
                latitude=row[5],
                longitude=row[6],
                sog_knots=row[7],
                cog_degrees=row[8],
                heading_degrees=row[9],
                nav_status=row[10],
                vessel_type=row[11],
            )
            events.append(event)

        batch_anomalies = self.detect_and_categorize_anomalies(events)
        self.calculate_window_metrics(events)
        self.upsert_to_postgis(clean_df, 0)
        self.persist_anomalies(batch_anomalies)

        # Publish clean events to ais.clean sink.
        for event in events:
            self._publish("ais.clean", str(event.mmsi), event.model_dump(mode="json"))

        # Publish detected anomalies to the canonical ais.anomaly sink.
        for anomaly in batch_anomalies:
            self._publish(
                "ais.anomaly",
                str(getattr(anomaly, "mmsi", None)),
                anomaly.to_dict() if hasattr(anomaly, "to_dict") else {"anomaly": str(anomaly)},
            )

        if self._predictors:
            for event in events:
                recent = [event.model_dump(mode="json")]
                for model_type, predictor in self._predictors.items():
                    try:
                        prediction = predictor.predict(recent).to_dict()
                        self._publish(
                            "ais.prediction",
                            str(event.mmsi),
                            {
                                "event_id": event.event_id,
                                "mmsi": event.mmsi,
                                "model_type": model_type,
                                "trained": predictor.trained,
                                "prediction": prediction,
                            },
                        )
                    except Exception as exc:
                        logger.error(
                            "prediction failed model=%s mmsi=%s: %s",
                            model_type,
                            event.mmsi,
                            exc,
                        )

        duration = (datetime.now(UTC) - t0).total_seconds() * 1000.0
        histogram("spark_batch_duration_ms", duration)
        logger.info("processed batch clean=%d invalid=%d", len(events), invalid_df.count() if invalid_df is not None else 0)


def main() -> None:
    """Main entry point for the AIS stream processor."""
    processor = AISProcessor()

    settings = get_settings()
    spark = SparkSession.builder.appName("ship-analytics").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    # Ensure checkpoint directory exists.
    checkpoint_dir = settings.checkpoint_dir
    Path(checkpoint_dir).mkdir(parents=True, exist_ok=True)
    logger.info("using checkpoint dir %s", checkpoint_dir)
    logger.info("kafka startingOffsets=%s", settings.kafka_starting_offset)

    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", settings.kafka_brokers)
        .option("subscribe", "ais.raw")
        .option("startingOffsets", settings.kafka_starting_offset)
    )
    for option, value in settings.spark_kafka_options().items():
        raw = raw.option(option, value)
    raw = raw.load()

    query = (
        raw.writeStream
        .foreachBatch(processor.process_event_batch)
        .outputMode("update")
        .option("checkpointLocation", checkpoint_dir)
        .start()
    )

    try:
        query.awaitTermination()
    except KeyboardInterrupt:
        query.stop()
        logger.info("stream processing stopped.")


if __name__ == "__main__":
    main()
