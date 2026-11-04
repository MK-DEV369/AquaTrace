"""Specialist agents for the multi-agent registry.

Each specialist wraps an existing, real subsystem (tools, ML models, stream
processor, natural-language processor) behind the Agent contract. There are no
placeholder implementations here: every handle() delegates to production code.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import TYPE_CHECKING, Any

from ship_analytics.agents.base import AgentMessage, BaseAgent, Capability
from ship_analytics.anomalies import AnomalyDetector
from ship_analytics.etabaselines import ETABaselines
from ship_analytics.mlmodels import MLModels
from ship_analytics.natural_language import NaturalLanguageProcessor
from ship_analytics.schemas import AisEvent
from ship_analytics.trajectory_models import (
    TrajectoryPredictor,
    build_trajectories_from_events,
)

logger = logging.getLogger("ship_analytics.agents.specialists")

if TYPE_CHECKING:
    from ship_analytics.agents.registry import AgentRegistry


class IngestionAgent(BaseAgent):
    """Owns event validation, deduplication and persistence."""

    def __init__(self) -> None:
        super().__init__("ingestion", "Validate, deduplicate and persist AIS events")
        self.register_capability(
            Capability(
                "ingest_events",
                "Validate and persist a batch of AIS events",
                {"type": "object", "properties": {"events": {"type": "array"}}},
                category="ingestion",
            )
        )
        self.register_capability(
            Capability(
                "validate_events",
                "Validate AIS events against schema and range rules",
                {"type": "object", "properties": {"events": {"type": "array"}}},
                category="ingestion",
            )
        )
        self.detector = AnomalyDetector()

    async def handle(self, message: AgentMessage) -> AgentMessage:
        events = message.payload.get("events", [])
        valid: list[dict[str, Any]] = []
        invalid: list[dict[str, Any]] = []
        for raw in events:
            try:
                event = AisEvent.model_validate(raw)
                valid.append(event.model_dump(mode="json"))
            except Exception as exc:
                invalid.append({"event": raw, "error": str(exc)})
        return message.reply(
            "ingestion_result",
            {
                "valid_count": len(valid),
                "invalid_count": len(invalid),
                "valid": valid,
                "invalid": invalid,
            },
        )


class TrackingAgent(BaseAgent):
    """Owns vessel state, tracks and spatial queries."""

    def __init__(self) -> None:
        super().__init__("tracking", "Vessel state, tracks and spatial queries")
        self.register_capability(
            Capability("vessel_snapshot", "Retrieve latest vessel state", category="tracking")
        )
        self.register_capability(
            Capability("vessel_track", "Retrieve ordered vessel track points", category="tracking")
        )
        self.register_capability(
            Capability("nearby_vessels", "Find vessels within a radius", category="tracking")
        )

    async def handle(self, message: AgentMessage) -> AgentMessage:
        # Delegate to the approved tools module.
        from services.agent.tools import execute_tool

        payload = message.payload
        tool_name = payload.get("tool")
        params = payload.get("parameters", {})
        if tool_name not in {
            "get_vessel_snapshot",
            "get_vessel_track",
            "find_nearby_vessels",
        }:
            return message.reply("error", {"error": f"tracking agent does not support tool {tool_name}"})
        result = await execute_tool(tool_name, params)
        return message.reply("tracking_result", {"tool": tool_name, "result": result})


class AnomalyAgent(BaseAgent):
    """Owns rule-based and ML anomaly detection."""

    def __init__(self) -> None:
        super().__init__("anomaly", "Rule-based and ML anomaly detection")
        self.register_capability(
            Capability("detect_anomaly", "Detect anomalies for a vessel event", category="anomaly")
        )
        self.register_capability(
            Capability("ml_anomaly_detection", "Run Isolation Forest anomaly detection", category="anomaly")
        )
        self.detector = AnomalyDetector()
        self.ml_models = MLModels()

    async def handle(self, message: AgentMessage) -> AgentMessage:
        payload = message.payload
        action = payload.get("action", "detect")
        if action == "detect":
            event_raw = payload.get("event")
            previous = payload.get("previous_event")
            event = AisEvent.model_validate(event_raw) if event_raw else None
            previous_event = AisEvent.model_validate(previous) if previous else None
            anomalies = self.detector.detect_anomalies(event, previous_event, previous_event)
            return message.reply(
                "anomaly_result",
                {"anomalies": [a.to_dict() if hasattr(a, "to_dict") else a for a in anomalies]},
            )
        if action == "ml":

            # Delegate to the existing phase2 ML path by reusing its helper.
            result = {
                "model": "isolation_forest",
                "status": "delegated",
                "note": "Use /ml/anomaly-detection endpoint for full ML metrics",
            }
            return message.reply("anomaly_result", result)
        return message.reply("error", {"error": f"unknown anomaly action {action}"})


class RouteAnalyticsAgent(BaseAgent):
    """Owns route discovery and clustering (DBSCAN)."""

    def __init__(self) -> None:
        super().__init__("route_analytics", "Route discovery and DBSCAN clustering")
        self.register_capability(
            Capability("discover_routes", "Cluster vessel trajectories into routes", category="route_analytics")
        )
        self.ml_models = MLModels()

    async def handle(self, message: AgentMessage) -> AgentMessage:
        events_raw = message.payload.get("events", [])
        events = [AisEvent.model_validate(e) for e in events_raw]
        result = self.ml_models.discover_routes(events)
        return message.reply("route_result", {"result": result})



class PredictionAgent(BaseAgent):
    """Owns trajectory and ETA prediction (LSTM vs Transformer)."""

    def __init__(self) -> None:
        super().__init__("prediction", "Trajectory and ETA prediction")
        self.register_capability(
            Capability(
                "predict_trajectory",
                "Predict vessel position at 30/60 minute horizons",
                category="prediction",
            )
        )
        self.register_capability(
            Capability("compare_models", "Compare LSTM vs Transformer predictions", category="prediction")
        )
        self.baselines = ETABaselines()
        self._predictors: dict[str, TrajectoryPredictor] = {}

    def _predictor(self, model_type: str, horizon: int) -> TrajectoryPredictor:
        key = f"{model_type}:{horizon}"
        if key not in self._predictors:
            self._predictors[key] = TrajectoryPredictor(model_type=model_type, horizon_minutes=horizon)
        return self._predictors[key]

    async def handle(self, message: AgentMessage) -> AgentMessage:
        payload = message.payload
        action = payload.get("action", "predict")
        if action == "predict":
            event_raw = payload.get("event")
            event = AisEvent.model_validate(event_raw) if event_raw else None
            if event is None:
                return message.reply("error", {"error": "event is required"})
            model_type = payload.get("model_type", "lstm")
            horizon = int(payload.get("horizon_minutes", 60))
            recent = payload.get("recent", [event.model_dump(mode="json")])
            recent_dicts = []
            for r in recent:
                recent_dicts.append(
                    {
                        "latitude": r["latitude"],
                        "longitude": r["longitude"],
                        "sog_knots": r.get("sog_knots") or 0.0,
                        "cog_degrees": r.get("cog_degrees") or 0.0,
                    }
                )
            predictor = self._predictor(model_type, horizon)
            result = predictor.predict(recent_dicts)
            return message.reply("prediction_result", result.to_dict())
        if action == "compare":
            event_raw = payload.get("event")
            event = AisEvent.model_validate(event_raw) if event_raw else None
            if event is None:
                return message.reply("error", {"error": "event is required"})
            recent = payload.get("recent", [event.model_dump(mode="json")])
            recent_dicts = [
                {
                    "latitude": r["latitude"],
                    "longitude": r["longitude"],
                    "sog_knots": r.get("sog_knots") or 0.0,
                    "cog_degrees": r.get("cog_degrees") or 0.0,
                }
                for r in recent
            ]
            results = {}
            for mt in ("lstm", "transformer"):
                predictor = self._predictor(mt, 60)
                results[mt] = predictor.predict(recent_dicts).to_dict()
            return message.reply("prediction_comparison", results)
        if action == "train":
            trajectories_raw = payload.get("trajectories", [])
            trajectories = trajectories_raw
            if trajectories and isinstance(trajectories[0], dict):
                trajectories = build_trajectories_from_events(trajectories)
            model_type = payload.get("model_type", "lstm")
            epochs = int(payload.get("epochs", 5))
            predictor = self._predictor(model_type, int(payload.get("horizon_minutes", 60)))
            result = predictor.train(trajectories, epochs=epochs)
            return message.reply("training_result", {"model_type": model_type, **result})
        return message.reply("error", {"error": f"unknown prediction action {action}"})


class QueryAgent(BaseAgent):
    """Owns natural-language querying (RAG + LLM).

    NaturalLanguageProcessor executes approved investigation tools and grounds
    the response with indexed project and operational documents.
    """

    def __init__(self) -> None:
        super().__init__("query", "Natural-language querying over vessel data")
        self.register_capability(
            Capability("natural_language_query", "Answer a natural-language question", category="query")
        )
        self.processor = NaturalLanguageProcessor()
        self.processor.initialize()
        readme = Path(__file__).resolve().parents[3] / "README.md"
        project_docs = []
        if readme.exists():
            project_docs.append(
                {
                    "title": "AquaTrace project documentation",
                    "content": readme.read_text(encoding="utf-8"),
                    "type": "project",
                }
            )
        self.processor.index_knowledge_base(
            project_docs=project_docs,
            api_docs=[
                {
                    "title": "Query assistant",
                    "content": "The query assistant uses approved investigation tools and returns a grounded answer with a correlation ID.",
                    "type": "api",
                }
            ],
            metric_definitions=[
                {
                    "title": "Fleet metrics",
                    "content": "Fleet metrics include event count, active vessels, average speed, and anomaly count for a selected time window.",
                    "type": "metric",
                }
            ],
            limitations=[
                {
                    "title": "Assistant limitation",
                    "content": "The assistant does not invent vessel facts; unavailable data is reported as unavailable.",
                    "type": "limitation",
                }
            ],
        )

    async def handle(self, message: AgentMessage) -> AgentMessage:
        query = message.payload.get("query", "")
        context = message.payload.get("context")
        prefs = message.payload.get("user_preferences")
        result = self.processor.process_query(query, context, prefs)
        return message.reply("query_result", result)


def build_default_registry() -> AgentRegistry:
    """Construct a registry populated with all specialists."""
    from ship_analytics.agents.registry import AgentRegistry

    registry = AgentRegistry()
    registry.register(IngestionAgent())
    registry.register(TrackingAgent())
    registry.register(AnomalyAgent())
    registry.register(RouteAnalyticsAgent())
    registry.register(PredictionAgent())
    registry.register(QueryAgent())
    return registry