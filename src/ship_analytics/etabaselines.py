"""ETA baseline models for Phase 2: Simple and trajectory prediction baselines."""

from datetime import datetime, timedelta
from typing import Dict, List, Optional

import numpy as np
import pandas as pd
from shapely.geometry import Point

from ship_analytics.schemas import AisEvent


class ETABaselines:
    """ETA baseline models for vessel position prediction."""

    def __init__(self):
        self.vessel_history = {}

    def predict_eta_simple_baseline(
        self,
        current_event: AisEvent,
        lookback_minutes: int = 120,
    ) -> Dict[str, float]:
        """Simple ETA baseline: remaining distance / robust recent speed.

        Args:
            current_event: Current vessel position
            lookback_minutes: Time window for recent speed calculation

        Returns:
            Dictionary with ETA predictions and confidence
        """
        # Get recent vessel events
        vessel_events = self.vessel_history.get(current_event.mmsi, [])
        recent_events = [
            e for e in vessel_events
            if (current_event.event_time - e.event_time).total_seconds() / 60 <= lookback_minutes
            and e.sog_knots is not None
        ]

        if len(recent_events) < 3:
            return self._get_default_eta_prediction(current_event)

        # Calculate robust average speed (remove outliers)
        speeds = [e.sog_knots for e in recent_events]
        speed_mean = np.mean(speeds)
        speed_std = np.std(speeds)

        # Remove outliers (>2 standard deviations)
        filtered_speeds = [
            s for s in speeds
            if abs(s - speed_mean) <= 2 * speed_std
        ]

        if not filtered_speeds:
            return self._get_default_eta_prediction(current_event)

        robust_speed = np.mean(filtered_speeds)

        # Calculate remaining distance to destination (simplified: use last known position as destination)
        remaining_distance = self._calculate_remaining_distance(current_event, vessel_events)

        # ETA calculation
        eta_hours = remaining_distance / robust_speed if robust_speed > 0 else None

        # Calculate confidence based on data quality
        confidence = self._calculate_eta_confidence(
            len(recent_events), len(filtered_speeds), robust_speed
        )

        return {
            "eta_hours": eta_hours,
            "eta_minutes": eta_hours * 60 if eta_hours else None,
            "remaining_distance_nm": remaining_distance,
            "robust_speed_knots": robust_speed,
            "recent_speed_samples": len(recent_events),
            "filtered_speed_samples": len(filtered_speeds),
            "confidence_score": confidence,
            "last_position_time": current_event.event_time.isoformat(),
            "model_type": "simple_baseline",
        }

    def predict_eta_trajectory_baseline(
        self,
        current_event: AisEvent,
        prediction_horizon_minutes: int = 60,
    ) -> Dict[str, object]:
        """Trajectory baseline: predict positions with constant velocity/course.

        Args:
            current_event: Current vessel position
            prediction_horizon_minutes: How far to predict

        Returns:
            Dictionary with predicted positions and accuracy metrics
        """
        # Get recent events for velocity calculation
        vessel_events = self.vessel_history.get(current_event.mmsi, [])
        recent_events = [
            e for e in vessel_events
            if (current_event.event_time - e.event_time).total_seconds() / 60 <= 60
            and e.sog_knots is not None and e.cog_degrees is not None
        ]

        if len(recent_events) < 2:
            return self._get_default_trajectory_prediction(current_event)

        # Calculate average velocity and course
        speeds = [e.sog_knots for e in recent_events]
        courses = [e.cog_degrees for e in recent_events]

        avg_speed = np.mean(speeds)
        avg_course = np.mean(courses)

        # Generate predictions for different horizons
        predictions = []
        for horizon_minutes in [15, 30, 60, prediction_horizon_minutes]:
            prediction = self._predict_position(
                current_event, avg_speed, avg_course, horizon_minutes
            )
            predictions.append(prediction)

        # Calculate accuracy metrics using validation data
        accuracy_metrics = self._calculate_trajectory_accuracy(
            current_event, recent_events, predictions
        )

        return {
            "predictions": predictions,
            "average_speed_knots": avg_speed,
            "average_course_degrees": avg_course,
            "recent_samples": len(recent_events),
            "accuracy_metrics": accuracy_metrics,
            "model_type": "trajectory_baseline",
        }

    def _predict_position(
        self,
        current_event: AisEvent,
        speed_knots: float,
        course_degrees: float,
        horizon_minutes: int,
    ) -> Dict[str, float]:
        """Predict vessel position at a given horizon."""
        # Convert speed to km/h
        speed_kmh = speed_knots * 1.852
        # Convert course to radians
        course_rad = np.radians(course_degrees)

        # Calculate distance traveled
        time_hours = horizon_minutes / 60
        distance_km = speed_kmh * time_hours

        # Calculate new position using haversine formula
        lat1, lon1 = np.radians(current_event.latitude), np.radians(current_event.longitude)

        # Destination coordinates
        dlat = distance_km / 6371.0
        dlon = distance_km / (6371.0 * np.cos(lat1))

        lat2 = np.degrees(lat1 + dlat * np.sin(course_rad))
        lon2 = np.degrees(lon1 + dlon * np.cos(course_rad))

        # Calculate bearing from current position
        bearing = np.degrees(np.arctan2(
            np.sin(course_rad) * np.cos(lat1),
            np.cos(lat1) * np.sin(lat2) - np.sin(lat1) * np.cos(lat2) * np.cos(dlon)
        ))

        return {
            "horizon_minutes": horizon_minutes,
            "predicted_latitude": lat2,
            "predicted_longitude": lon2,
            "predicted_speed_knots": speed_knots,
            "predicted_course_degrees": course_degrees,
            "bearing_from_current": bearing,
            "distance_nm": distance_km / 1.852,  # Convert to nautical miles
        }

    def _calculate_remaining_distance(
        self,
        current_event: AisEvent,
        vessel_events: List[AisEvent],
    ) -> float:
        """Calculate remaining distance to destination.

        For simplicity, we use the last known position as destination.
        In a real implementation, this would use port coordinates or
        intended destination information.
        """
        if not vessel_events:
            return 0.0

        # Use the last position before current as destination
        prev_events = [
            e for e in vessel_events
            if e.event_time < current_event.event_time
        ]

        if not prev_events:
            return 0.0

        last_known = max(prev_events, key=lambda e: e.event_time)

        # Calculate great-circle distance
        distance = self._haversine_distance(
            current_event.latitude, current_event.longitude,
            last_known.latitude, last_known.longitude
        )

        return distance

    def _calculate_trajectory_accuracy(
        self,
        current_event: AisEvent,
        recent_events: List[AisEvent],
        predictions: List[Dict],
    ) -> Dict[str, float]:
        """Calculate trajectory prediction accuracy."""
        if len(recent_events) < 3:
            return {
                "mae_nm": None,
                "median_absolute_error_nm": None,
                "p95_geodesic_error_nm": None,
            }

        # Use most recent position as reference
        most_recent = recent_events[-1]

        errors = []
        for pred in predictions:
            error = self._haversine_distance(
                current_event.latitude, current_event.longitude,
                pred["predicted_latitude"], pred["predicted_longitude"]
            )
            errors.append(error)

        if not errors:
            return {
                "mae_nm": None,
                "median_absolute_error_nm": None,
                "p95_geodesic_error_nm": None,
            }

        return {
            "mae_nm": np.mean(errors),
            "median_absolute_error_nm": np.median(errors),
            "p95_geodesic_error_nm": np.percentile(errors, 95),
            "mean_error_nm": np.mean(errors),
            "std_error_nm": np.std(errors),
        }

    def _calculate_eta_confidence(
        self,
        speed_samples: int,
        filtered_samples: int,
        robust_speed: float,
    ) -> float:
        """Calculate confidence score for ETA prediction."""
        # Base confidence on sample size
        sample_confidence = min(speed_samples / 10.0, 1.0)  # Up to 1.0 with 10+ samples

        # Reduce confidence if many outliers were filtered
        filter_penalty = 1.0 - (speed_samples - filtered_samples) / max(speed_samples, 1)

        # Speed stability confidence
        speed_stability = 1.0 - min(robust_speed / 60.0, 1.0)  # Lower speed = higher confidence

        # Combine confidence factors
        confidence = (sample_confidence * 0.5 + filter_penalty * 0.3 + speed_stability * 0.2)

        return max(0.0, min(1.0, confidence))

    def _get_default_eta_prediction(
        self,
        current_event: AisEvent,
    ) -> Dict[str, float]:
        """Return default ETA prediction when insufficient data."""
        return {
            "eta_hours": None,
            "eta_minutes": None,
            "remaining_distance_nm": 0.0,
            "robust_speed_knots": current_event.sog_knots or 0.0,
            "recent_speed_samples": 0,
            "filtered_speed_samples": 0,
            "confidence_score": 0.0,
            "last_position_time": current_event.event_time.isoformat(),
            "model_type": "simple_baseline",
            "insufficient_data": True,
        }

    def _get_default_trajectory_prediction(
        self,
        current_event: AisEvent,
    ) -> Dict[str, object]:
        """Return default trajectory prediction when insufficient data."""
        return {
            "predictions": [],
            "average_speed_knots": current_event.sog_knots or 0.0,
            "average_course_degrees": current_event.cog_degrees or 0.0,
            "recent_samples": 0,
            "accuracy_metrics": {
                "mae_nm": None,
                "median_absolute_error_nm": None,
                "p95_geodesic_error_nm": None,
            },
            "model_type": "trajectory_baseline",
            "insufficient_data": True,
        }

    def _haversine_distance(self, lat1: float, lon1: float, lat2: float, lon2: float) -> float:
        """Calculate great-circle distance between two points in kilometers."""
        from math import radians, sin, cos, sqrt, atan2

        R = 6371  # Earth radius in km

        lat1, lon1, lat2, lon2 = map(radians, [lat1, lon1, lat2, lon2])

        dlat = lat2 - lat1
        dlon = lon2 - lon1

        a = sin(dlat / 2) ** 2 + cos(lat1) * cos(lat2) * sin(dlon / 2) ** 2
        c = 2 * atan2(sqrt(a), sqrt(1 - a))

        return R * c

    def update_vessel_history(self, event: AisEvent) -> None:
        """Update vessel history with new event."""
        if event.mmsi not in self.vessel_history:
            self.vessel_history[event.mmsi] = []

        # Add event to history
        self.vessel_history[event.mmsi].append(event)

        # Keep only last 24 hours of history
        cutoff_time = event.event_time - timedelta(hours=24)
        self.vessel_history[event.mmsi] = [
            e for e in self.vessel_history[event.mmsi]
            if e.event_time >= cutoff_time
        ]

    def train_baselines(self, events: List[AisEvent]) -> Dict[str, Dict]:
        """Train baseline models on given events."""
        baselines = {}

        # Group events by vessel
        for event in events:
            self.update_vessel_history(event)

        # Generate predictions for each vessel
        for vessel_mmsi, vessel_events in self.vessel_history.items():
            if len(vessel_events) < 2:
                continue

            # Sort by time
            vessel_events.sort(key=lambda e: e.event_time)

            # Generate predictions for recent events
            recent_events = vessel_events[-5:]  # Last 5 events for evaluation

            for event in recent_events:
                simple_pred = self.predict_eta_simple_baseline(event)
                trajectory_pred = self.predict_eta_trajectory_baseline(event)

                baselines[f"{vessel_mmsi}_{event.event_time}"] = {
                    "simple_baseline": simple_pred,
                    "trajectory_baseline": trajectory_pred,
                }

        return baselines

    def evaluate_baselines(
        self,
        test_events: List[AisEvent],
        simple_baseline_results: Dict,
        trajectory_baseline_results: Dict,
    ) -> Dict[str, Dict[str, float]]:
        """Evaluate baseline performance on test data."""
        evaluation_results = {
            "simple_baseline": {
                "mae_nm": 0.0,
                "median_absolute_error_nm": 0.0,
                "p95_geodesic_error_nm": 0.0,
                "confidence_mean": 0.0,
            },
            "trajectory_baseline": {
                "mae_nm": 0.0,
                "median_absolute_error_nm": 0.0,
                "p95_geodesic_error_nm": 0.0,
                "samples_evaluated": 0,
            },
        }

        # TODO: Implement proper evaluation logic
        # This would compare predictions against actual positions

        return evaluation_results
