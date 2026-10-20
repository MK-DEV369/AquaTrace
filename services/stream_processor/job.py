"""Stream processor job for Phase 1: validate, deduplicate, publish, and persist AIS events."""

from datetime import timezone
import psycopg
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, from_json, lit, struct, to_json
from pyspark.sql.types import DoubleType, IntegerType, StringType, StructField, StructType, TimestampType

from ship_analytics.config import get_settings
from ship_analytics.schemas import AisEvent
from ship_analytics.anomalies import AnomalyDetector
from ship_analytics.windows import WindowCalculator

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


class AISProcessor:
    """Main processing pipeline for AIS events."""

    def __init__(self):
        self.detector = AnomalyDetector()
        self.window_calculator = WindowCalculator(window_minutes=60)
        self.processed_events = []
        self.anomalies = []
        self.vessel_states = {}

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
        for event in events:
            previous_event = self.vessel_states.get(event.mmsi)
            last_known_state = self.vessel_states.get(event.mmsi)

            anomalies = self.detector.detect_anomalies(
                event, previous_event, last_known_state
            )
            self.anomalies.extend(anomalies)

            self.vessel_states[event.mmsi] = event

        return self.anomalies

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

        settings = get_settings()
        with psycopg.connect(settings.database_url) as conn, conn.cursor() as cursor:
            for event in events:
                ais_event = AisEvent(
                    event_id=event["event_id"],
                    source=event["source"],
                    mmsi=event["mmsi"],
                    event_time=event["event_time"],
                    ingested_at=event["ingested_at"],
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

    def process_event_batch(self, batch_df: DataFrame, _: int) -> None:
        """Process a batch of events through the full pipeline."""
        clean_df, invalid_df = self.validate_events(batch_df)

        events = []
        for row in clean_df.select(*EVENT_SCHEMA.fieldNames()).toLocalIterator():
            event = AisEvent(
                event_id=row[0],
                source=row[1],
                mmsi=row[2],
                event_time=row[3].replace(tzinfo=timezone.utc),
                ingested_at=row[4].replace(tzinfo=timezone.utc),
                latitude=row[5],
                longitude=row[6],
                sog_knots=row[7],
                cog_degrees=row[8],
                heading_degrees=row[9],
                nav_status=row[10],
                vessel_type=row[11],
            )
            events.append(event)

        self.detect_and_categorize_anomalies(events)
        self.calculate_window_metrics(events)
        self.upsert_to_postgis(batch_df, 0)


def main() -> None:
    """Main entry point for the AIS stream processor."""
    processor = AISProcessor()

    settings = get_settings()
    spark = SparkSession.builder.appName("ship-analytics").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", settings.kafka_brokers)
        .option("subscribe", "ais.raw")
        .option("startingOffsets", "earliest")
        .load()
    )

    query = (
        raw.writeStream
        .foreachBatch(processor.process_event_batch)
        .outputMode("update")
        .option("checkpointLocation", "checkpoints")
        .start()
    )

    try:
        query.awaitTermination()
    except KeyboardInterrupt:
        query.stop()
        print("\nStream processing stopped.")


if __name__ == "__main__":
    main()
