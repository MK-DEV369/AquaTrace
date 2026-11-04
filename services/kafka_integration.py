"""Kafka integration for AquaTrace AI Maritime Investigation Agent.

This module integrates the agentic AI system with the existing Kafka infrastructure,
providing real-time data consumption and investigation capabilities.
"""

from __future__ import annotations

import asyncio
import json
from datetime import datetime
from typing import Any

from confluent_kafka import Consumer, KafkaError, Producer

from services.agent.tools import execute_tool
from ship_analytics.config import get_settings
from ship_analytics.logging import get_logger
from ship_analytics.metrics import counter
from ship_analytics.schemas import AisEvent

logger = get_logger("kafka_integration")


def _kafka_client_config(settings, client_id: str) -> dict[str, Any]:
    config: dict[str, Any] = {
        "bootstrap.servers": settings.kafka_brokers,
        "client.id": client_id,
    }
    protocol = settings.kafka_security_protocol
    config["security.protocol"] = protocol
    if protocol in {"SASL_PLAINTEXT", "SASL_SSL"}:
        if settings.kafka_sasl_mechanism:
            config["sasl.mechanism"] = settings.kafka_sasl_mechanism
        if settings.kafka_sasl_username:
            config["sasl.username"] = settings.kafka_sasl_username
        if settings.kafka_sasl_password:
            config["sasl.password"] = settings.kafka_sasl_password
    if protocol in {"SSL", "SASL_SSL"} and settings.kafka_ssl_ca_location:
        config["ssl.ca.location"] = settings.kafka_ssl_ca_location
    return config


class KafkaInvestigationConsumer:
    """Kafka consumer for real-time vessel investigation data."""

    def __init__(self, topic: str = "ais.raw", group_id: str = "agent-investigation"):
        self.topic = topic
        self.group_id = group_id
        self.consumer = None
        self.settings = get_settings()
        self.active_investigations: dict[str, dict[str, Any]] = {}
        self._dlq_producer: Producer | None = None

    async def start_consumption(self):
        """Start consuming messages from Kafka."""
        config = _kafka_client_config(self.settings, "agent-consumer")
        config.update(
            {
                "group.id": self.group_id,
                "auto.offset.reset": "latest",
                "enable.auto.commit": False,
                "max.poll.interval.ms": 300000,
                "session.timeout.ms": 300000,
                "isolation.level": "read_committed",
            }
        )

        self.consumer = Consumer(config)
        self.consumer.subscribe([self.topic])
        self._dlq_producer = Producer(_kafka_client_config(self.settings, "agent-dlq"))

        logger.info("started consuming topic=%s group=%s", self.topic, self.group_id)

        while True:
            try:
                message = self.consumer.poll(timeout=1.0)
                if message is None:
                    continue

                if message.error():
                    if message.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    logger.error("kafka error: %s", message.error())
                    continue

                await self.process_message(message)

            except Exception as e:
                logger.exception("error in kafka consumption: %s", e)
                await asyncio.sleep(1)

    async def process_message(self, message):
        """Process a Kafka message with retry and dead-letter handling."""
        last_exc: Exception | None = None
        for attempt in range(1, self.settings.kafka_max_retries + 1):
            try:
                raw_value = message.value().decode("utf-8")
                event_data = json.loads(raw_value)
                event = AisEvent(**event_data)

                investigation_triggers = await self.check_investigation_triggers(event)
                if investigation_triggers:
                    logger.info("investigation triggered vessel=%s", event.mmsi)
                    await self.trigger_investigation(event, investigation_triggers)

                await self.store_event_for_analysis(event)

                try:
                    self.consumer.commit(message)
                except Exception as exc:  # pragma: no cover - defensive
                    logger.warning("commit failed: %s", exc)
                counter("kafka_messages_processed_total", labels={"topic": self.topic})
                return
            except Exception as exc:
                last_exc = exc
                logger.warning(
                    "process attempt %d/%d failed: %s",
                    attempt,
                    self.settings.kafka_max_retries,
                    exc,
                )
                await asyncio.sleep(min(2 ** attempt, 8))

        # Exhausted retries -> dead letter.
        logger.error("message failed after retries; sending to DLQ: %s", last_exc)
        self._send_to_dlq(message, last_exc)
        try:
            self.consumer.commit(message)
        except Exception:  # pragma: no cover - defensive
            pass
        counter("kafka_messages_dlq_total", labels={"topic": self.topic})

    def _send_to_dlq(self, message, exc: Exception | None) -> None:
        if self._dlq_producer is None:
            return
        try:
            payload = {
                "original_topic": message.topic(),
                "partition": message.partition(),
                "offset": message.offset(),
                "value": message.value().decode("utf-8", errors="replace"),
                "error": str(exc) if exc else None,
                "timestamp": datetime.utcnow().isoformat(),
            }
            self._dlq_producer.produce(
                self.settings.kafka_dlq_topic,
                key=message.key(),
                value=json.dumps(payload),
            )
            self._dlq_producer.poll(0)
        except Exception as e:  # pragma: no cover - defensive
            logger.error("failed to publish to DLQ: %s", e)

    async def check_investigation_triggers(self, event: AisEvent) -> list[str]:
        """Check if an event should trigger an investigation."""
        triggers = []

        if event.sog_knots is not None and event.sog_knots > 50:
            triggers.append("high_speed_event")

        if event.cog_degrees is not None and event.cog_degrees > 180:
            triggers.append("sharp_course_change")

        for session_id, session_info in self.active_investigations.items():
            if session_info.get("vessel_mmsi") == event.mmsi:
                triggers.append(f"investigation_session:{session_id}")

        return triggers

    async def trigger_investigation(self, event: AisEvent, triggers: list[str]):
        """Trigger an investigation based on event triggers."""
        for trigger in triggers:
            if trigger.startswith("investigation_session:"):
                session_id = trigger.split(":")[1]
                session_info = self.active_investigations.get(session_id)
                if session_info:
                    await self.update_investigation_session(session_id, event)

            elif trigger == "high_speed_event":
                await self.auto_investigate_event(event, "high_speed_anomaly")

            elif trigger == "sharp_course_change":
                await self.auto_investigate_event(event, "sharp_course_change")

    async def update_investigation_session(self, session_id: str, event: AisEvent):
        """Update an investigation session with new event data."""
        session_info = self.active_investigations.get(session_id)
        if session_info:
            session_info["events"].append(event)
            session_info["last_updated"] = datetime.utcnow()

            if len(session_info["events"]) >= session_info.get("min_events", 5):
                await self.analyze_investigation_session(session_id)

    async def auto_investigate_event(self, event: AisEvent, anomaly_type: str):
        """Automatically investigate an event that triggered a rule."""
        try:
            from services.agent.investigation_agent import (
                InvestigationRequest,
                MaritimeInvestigationAgent,
            )

            question = (
                f"Investigate why vessel {event.mmsi} triggered a {anomaly_type}. "
                f"Event occurred at {event.event_time}, position {event.latitude}, {event.longitude}, "
                f"speed {event.sog_knots} knots, course {event.cog_degrees} degrees."
            )

            request = InvestigationRequest(
                question=question,
                context={
                    "user_id": "auto_detector",
                    "anomaly_type": anomaly_type,
                    "event_source": "kafka_consumer",
                },
                max_tools=5,
                reasoning_depth="standard",
            )

            agent = MaritimeInvestigationAgent()
            session_id = await agent.start_investigation(request)

            self.active_investigations[session_id] = {
                "vessel_mmsi": event.mmsi,
                "anomaly_type": anomaly_type,
                "start_time": datetime.utcnow(),
                "events": [event],
                "min_events": 3,
                "auto_triggered": True,
            }

            await agent.execute_investigation(session_id)
            logger.info("auto-investigation completed vessel=%s session=%s", event.mmsi, session_id)

        except Exception as e:
            logger.exception("error in auto-investigation: %s", e)

    async def store_event_for_analysis(self, event: AisEvent):
        """Store event for analysis and future investigations."""
        pass

    async def analyze_investigation_session(self, session_id: str):
        """Analyze a completed investigation session."""
        session_info = self.active_investigations.get(session_id)
        if not session_info:
            return

        try:
            vessel_snapshot = await execute_tool(
                "get_vessel_snapshot", {"mmsi": session_info["vessel_mmsi"]}
            )
            recent_anomalies = await execute_tool(
                "get_recent_anomalies",
                {"mmsi": session_info["vessel_mmsi"], "time_range": "24 hours"},
            )
            logger.info("investigation analysis completed session=%s", session_id)
            _ = vessel_snapshot, recent_anomalies
        except Exception as e:
            logger.exception("error analyzing investigation session: %s", e)

    async def get_active_sessions(self) -> dict[str, Any]:
        """Get all active investigation sessions."""
        return self.active_investigations.copy()

    async def stop(self):
        """Stop the Kafka consumer."""
        if self.consumer:
            self.consumer.close()
            logger.info("kafka consumer stopped")
        if self._dlq_producer:
            self._dlq_producer.flush()


class KafkaInvestigationProducer:
    """Kafka producer for investigation results and alerts."""

    def __init__(self):
        self.producer = None
        self.settings = get_settings()

    async def start(self):
        """Initialize the Kafka producer."""
        self.producer = Producer(_kafka_client_config(self.settings, "agent-producer"))
        logger.info("kafka producer initialized")

    async def publish_investigation_result(self, session_id: str, investigation_result: dict[str, Any]):
        """Publish investigation results to Kafka."""
        try:
            message = {
                "type": "investigation_result",
                "session_id": session_id,
                "timestamp": datetime.utcnow().isoformat(),
                "result": investigation_result,
            }

            self.producer.produce(
                "agent.investigations",
                key=session_id,
                value=json.dumps(message),
            )
            self.producer.poll(0)
            logger.info("investigation result published session=%s", session_id)

        except Exception as e:
            logger.exception("error publishing investigation result: %s", e)

    async def publish_investigation_alert(self, alert: dict[str, Any]):
        """Publish investigation alert to Kafka."""
        try:
            message = {
                "type": "investigation_alert",
                "timestamp": datetime.utcnow().isoformat(),
                "alert": alert,
            }

            self.producer.produce(
                "agent.alerts",
                key=alert.get("session_id", "unknown"),
                value=json.dumps(message),
            )
            self.producer.poll(0)
            logger.info("investigation alert published message=%s", alert.get("message", "Unknown alert"))

        except Exception as e:
            logger.exception("error publishing investigation alert: %s", e)

    async def stop(self):
        """Stop the Kafka producer."""
        if self.producer:
            self.producer.flush()
            logger.info("kafka producer stopped")


# Global instances
kafka_consumer = KafkaInvestigationConsumer()
kafka_producer = KafkaInvestigationProducer()

investigation_agent = None  # Lazily created to avoid circular imports.


def _get_investigation_agent():
    global investigation_agent
    if investigation_agent is None:
        from services.agent.investigation_agent import MaritimeInvestigationAgent

        investigation_agent = MaritimeInvestigationAgent()
    return investigation_agent


async def setup_kafka_integration():
    """Setup and initialize Kafka integration for agentic AI."""
    logger.info("setting up kafka integration")

    consumer_task = asyncio.create_task(kafka_consumer.start_consumption())
    await kafka_producer.start()

    logger.info(
        "kafka integration ready consumer_topic=%s group=%s",
        kafka_consumer.topic,
        kafka_consumer.group_id,
    )
    return consumer_task


async def stop_kafka_integration():
    """Stop Kafka integration."""
    logger.info("stopping kafka integration")
    await kafka_producer.stop()
    await kafka_consumer.stop()
    logger.info("kafka integration stopped")


async def get_kafka_status() -> dict[str, Any]:
    """Get Kafka integration status."""
    return {
        "consumer_active": kafka_consumer.consumer is not None,
        "producer_active": kafka_producer.producer is not None,
        "active_investigations": len(kafka_consumer.active_investigations),
        "topic": kafka_consumer.topic,
        "group_id": kafka_consumer.group_id,
    }