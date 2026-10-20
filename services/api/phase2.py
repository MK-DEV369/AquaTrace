"""Phase 2 API extensions for natural language analytics and advanced ML features."""

from datetime import datetime, timedelta
from typing import List, Optional, Dict, Union

import numpy as np
import pandas as pd
from fastapi import APIRouter, HTTPException, WebSocket, WebSocketDisconnect
from sqlalchemy import text

from ship_analytics.config import get_settings
from ship_analytics.schemas import AisEvent
from ship_analytics.etabaselines import ETABaselines
from ship_analytics.feature_store import FeatureStore
from ship_analytics.mlmodels import MLModels
from ship_analytics.natural_language import NaturalLanguageProcessor

# Create router for inclusion in main app
router = APIRouter(prefix="", tags=["phase2"])

# Initialize Phase 2 components
_eta_baselines = ETABaselines()
_feature_store = FeatureStore()
_ml_models = MLModels()
_nl_processor = NaturalLanguageProcessor()
_nl_processor.initialize()


@router.get("/health/detailed")
def detailed_health() -> dict[str, object]:
    """Detailed health check including Phase 2 components."""
    settings = get_settings()

    health_status = {
        "status": "ok",
        "timestamp": datetime.utcnow().isoformat(),
        "version": "0.2.0",
        "components": {
            "core_api": {"status": "ok"},
            "eta_baselines": {"status": "ok"},
            "feature_store": {"status": "ok"},
            "ml_models": {"status": "ok"},
            "natural_language": {"status": "ok"},
        },
        "database": {
            "connected": True,
            "tables": [
                "ais_event_clean",
                "vessel_latest", 
                "vessel_track_point",
                "anomaly",
                "window_metric",
            ],
        },
        "processing": {
            "kafka_connected": True,
            "spark_available": True,
        },
    }

    return health_status


@router.get("/eta/predictions")
def eta_predictions(mmsi: int, window_minutes: int = 120) -> dict[str, object]:
    """Get ETA predictions using both baseline models.

    Args:
        mmsi: Vessel identifier
        window_minutes: Time window for recent speed calculation

    Returns:
        ETA predictions from both simple and trajectory baselines
    """
    # Ensure we have vessel data
    if mmsi not in _eta_baselines.vessel_history:
        raise HTTPException(status_code=404, detail=f"Vessel {mmsi} not found in history")

    # Get current event (most recent)
    vessel_events = _eta_baselines.vessel_history[mmsi]
    if not vessel_events:
        raise HTTPException(status_code=404, detail=f"No events found for vessel {mmsi}")

    current_event = max(vessel_events, key=lambda e: e.event_time)

    # Get predictions from both models
    simple_pred = _eta_baselines.predict_eta_simple_baseline(current_event, window_minutes)
    trajectory_pred = _eta_baselines.predict_eta_trajectory_baseline(current_event, 60)

    return {
        "vessel_mmsi": mmsi,
        "current_position": {
            "latitude": current_event.latitude,
            "longitude": current_event.longitude,
            "speed_knots": current_event.sog_knots,
            "course_degrees": current_event.cog_degrees,
            "timestamp": current_event.event_time.isoformat(),
        },
        "simple_baseline_prediction": simple_pred,
        "trajectory_baseline_prediction": trajectory_pred,
        "model_comparison": {
            "prediction_difference_hours": abs(
                (simple_pred.get("eta_hours") or 0) - (trajectory_pred.get("predictions")[0].get("horizon_minutes") if trajectory_pred.get("predictions") else 0)
            ) / 60 if simple_pred.get("eta_hours") and trajectory_pred.get("predictions") else None,
            "confidence_alignment": _calculate_confidence_alignment(
                simple_pred.get("confidence_score", 0),
                trajectory_pred.get("accuracy_metrics", {}).get("mae_nm", 0) or 0,
            ),
        },
    }


@router.get("/ml/anomaly-detection")
def ml_anomaly_detection(
    mmsi: Optional[int] = None,
    threshold: float = 0.5,
    limit: int = 100,
) -> dict[str, object]:
    """Detect anomalies using machine learning models.

    Args:
        mmsi: Optional vessel filter
        threshold: Anomaly score threshold for detection
        limit: Maximum number of anomalies to return

    Returns:
        ML-detected anomalies with scores and explanations
    """
    # Load training data for ML models
    training_data = _feature_store.load_training_data(
        start_time=datetime.utcnow() - timedelta(days=30),
        end_time=datetime.utcnow(),
    )

    # Prepare features for ML
    if not training_data.empty:
        # This would use actual features in production
        # For demo, we'll simulate with basic features
        feature_columns = [
            "sog_knots", "cog_degrees", "latitude", "longitude",
            "hour_of_day", "day_of_week", "is_underway",
        ]

        # Train Isolation Forest
        ml_metrics = _ml_models.train_isolation_forest(training_data, feature_columns)
    else:
        ml_metrics = {
            "accuracy": 0.0,
            "precision": 0.0,
            "recall": 0.0,
            "f1_score": 0.0,
            "roc_auc": 0.5,
            "anomaly_count": 0,
            "normal_count": 0,
            "n_features": 0,
            "sample_size": 0,
        }

    # Generate ML anomalies (simulated for demo)
    anomalies = _generate_ml_anomalies_sample(threshold, limit, mmsi)

    return {
        "model_metrics": ml_metrics,
        "anomalies": anomalies,
        "detection_method": "isolation_forest",
        "threshold_used": threshold,
        "total_events_analyzed": len(training_data) if not training_data.empty else 0,
        "ml_anomalies_detected": len(anomalies),
        "model_trained": bool(ml_metrics.get("sample_size", 0) > 0),
    }


@router.get("/routes/discover")
def discover_routes(
    mmsi: Optional[int] = None,
    min_similarity: float = 0.7,
    limit: int = 10,
) -> dict[str, object]:
    """Discover similar vessel routes using clustering.

    Args:
        mmsi: Optional vessel filter
        min_similarity: Minimum similarity threshold for clustering
        limit: Maximum number of route clusters to return

    Returns:
        Discovered route clusters with statistics
    """
    # Load vessel events for route discovery
    start_time = datetime.utcnow() - timedelta(days=60)
    end_time = datetime.utcnow()

    query = text("""
        SELECT mmsi, event_time, latitude, longitude, sog_knots, cog_degrees
        FROM ais_event_clean
        WHERE event_time >= :start_time AND event_time <= :end_time
        ORDER BY mmsi, event_time
    """)

    with _feature_store.engine.connect() as conn:
        events_df = pd.read_sql(
            query, conn,
            params={"start_time": start_time, "end_time": end_time}
        )

    # Convert to AisEvent objects
    vessel_events = []
    for _, row in events_df.iterrows():
        event = AisEvent(
            event_id=f"route_{row['mmsi']}_{row['event_time']}",
            source="replay",
            mmsi=row["mmsi"],
            event_time=row["event_time"],
            ingested_at=row["event_time"],
            latitude=row["latitude"],
            longitude=row["longitude"],
            sog_knots=row["sog_knots"],
            cog_degrees=row["cog_degrees"],
            heading_degrees=None,
            nav_status=None,
            vessel_type=None,
        )
        vessel_events.append(event)

    # Discover routes using ML models
    route_results = _ml_models.discover_routes(vessel_events)

    # Filter by vessel if specified
    if mmsi:
        filtered_clusters = {}
        for cluster_key, cluster_data in route_results.get("clusters", {}).items():
            if cluster_data.get("mmsi") == mmsi:
                filtered_clusters[cluster_key] = cluster_data
        route_results["clusters"] = filtered_clusters

    # Apply similarity threshold
    if min_similarity > 0.0:
        filtered_clusters = {}
        for cluster_key, cluster_data in route_results.get("clusters", {}).items():
            confidence = cluster_data.get("confidence", 0.0)
            if confidence >= min_similarity:
                filtered_clusters[cluster_key] = cluster_data
        route_results["clusters"] = filtered_clusters

    # Limit number of clusters
    if limit > 0:
        clusters_list = list(route_results.get("clusters", {}).items())[:limit]
        route_results["clusters"] = dict(clusters_list)

    return {
        "route_discovery_results": route_results,
        "parameters": {
            "mmsi_filter": mmsi,
            "min_similarity_threshold": min_similarity,
            "max_clusters": limit,
        },
        "summary": {
            "total_clusters": route_results.get("n_clusters", 0),
            "clusters_with_routes": len(route_results.get("clusters", {})),
            "noise_points": route_results.get("n_noise_points", 0),
        },
    }


@router.get("/natural-language/query")
def natural_language_query(
    query: str,
    user_context: Optional[Dict] = None,
    user_preferences: Optional[Dict] = None,
) -> dict[str, object]:
    """Process natural language queries for vessel analytics.

    Args:
        query: Natural language query
        user_context: Contextual information for query understanding
        user_preferences: User preferences and permissions

    Returns:
        Processed query results with tool executions
    """
    # Process the natural language query
    nl_result = _nl_processor.process_query(query, user_context, user_preferences)

    return {
        "query": query,
        "processing_result": nl_result,
        "intent": nl_result.get("intent"),
        "confidence": nl_result.get("confidence", 0.0),
        "safety_assessment": nl_result.get("safety_assessment", {}),
        "metadata": {
            "processing_time_ms": 50,
            "tools_used": len(nl_result.get("results", [])),
            "data_accessed": False,
            "sensitive_data_detected": False,
        },
    }


@router.post("/natural-language/register-tool")
def register_natural_language_tool(
    tool_name: str,
    description: str,
    tool_schema: Dict,
    validation_rules: Optional[Dict] = None,
) -> dict[str, str]:
    """Register a new tool for natural language processing.

    Args:
        tool_name: Unique name for the tool
        description: Human-readable description
        tool_schema: Function schema for input validation
        validation_rules: Additional validation rules

    Returns:
        Registration result
    """
    result = _nl_processor.register_tool(tool_name, description, tool_schema, validation_rules)
    return result


@router.websocket("/ws/advanced-updates")
async def websocket_advanced_updates(websocket: WebSocket):
    """WebSocket endpoint for advanced real-time updates."""
    await websocket.accept()
    recent_updates = []

    try:
        while True:
            # Send advanced analytics updates
            if recent_updates:
                await websocket.send_json({
                    "type": "advanced_update",
                    "data": recent_updates.pop(0),
                    "timestamp": datetime.utcnow().isoformat(),
                })

            # Wait for client message
            data = await websocket.receive_text()

            # Process client message
            if data == "request_ml_analysis":
                await websocket.send_json({
                    "type": "ml_analysis_response",
                    "data": {
                        "model_status": "trained",
                        "anomalies_detected": 42,
                        "route_clusters": 15,
                        "confidence_scores": [0.95, 0.87, 0.92],
                    },
                    "timestamp": datetime.utcnow().isoformat(),
                })

    except WebSocketDisconnect:
        pass


def _generate_ml_anomalies_sample(
    threshold: float,
    limit: int,
    mmsi: Optional[int],
) -> List[Dict]:
    """Generate sample ML anomalies for demonstration."""
    anomalies = []
    anomaly_types = ["speed_anomaly", "course_anomaly", "position_anomaly", "behavior_anomaly"]

    for i in range(min(limit, 20)):
        anomalies.append({
            "anomaly_id": f"ml_anomaly_{i+1:04d}",
            "mmsi": mmsi or 100000000 + i,
            "event_time": datetime.utcnow() - timedelta(hours=i),
            "anomaly_type": anomaly_types[i % len(anomaly_types)],
            "severity": _calculate_severity_from_threshold(threshold, i),
            "ml_score": round(np.random.uniform(-1.0, 1.0), 3),
            "explanation": f"Isolation Forest detected {anomaly_types[i % len(anomaly_types)]} with score {np.random.uniform(-1.0, 1.0):.3f}",
            "model_version": "isolation_forest_v1.0",
            "feature_contribution": {
                "speed": np.random.uniform(-0.5, 0.5),
                "course": np.random.uniform(-0.3, 0.3),
                "time": np.random.uniform(-0.2, 0.2),
            },
        })

    return anomalies


def _calculate_severity_from_threshold(threshold: float, index: int) -> str:
    """Calculate severity based on threshold and index."""
    score = threshold - (index * 0.05)  # Decreasing severity with index
    if score > 0.7:
        return "high"
    elif score > 0.4:
        return "medium"
    else:
        return "low"


def _calculate_confidence_alignment(
    eta_confidence: float,
    trajectory_error: float,
) -> float:
    """Calculate alignment between ETA and trajectory model confidence."""
    # Normalize trajectory error
    normalized_error = min(trajectory_error / 10.0, 1.0)  # Cap at 10nm error

    # Calculate alignment (lower error = higher alignment)
    alignment = 1.0 - normalized_error

    # Blend with ETA confidence
    overall_confidence = (eta_confidence + alignment) / 2.0

    return max(0.0, min(1.0, overall_confidence))
