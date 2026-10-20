"""ML models for Phase 2: Isolation Forest anomaly detection and route discovery."""


import numpy as np
import pandas as pd
from sklearn.cluster import DBSCAN
from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import StandardScaler

from ship_analytics.schemas import AisEvent


class MLModels:
    """Machine learning models for anomaly detection and route analysis."""

    def __init__(self):
        self.isolation_forest = IsolationForest(
            n_estimators=100,
            contamination=0.1,
            random_state=42,
            n_jobs=-1,
        )
        self.scaler = StandardScaler()
        self.route_clusterer = DBSCAN(eps=0.5, min_samples=5, metric='euclidean')
        self.feature_store = None

    def train_isolation_forest(
        self,
        training_data: pd.DataFrame,
        feature_columns: list[str],
    ) -> dict[str, float]:
        """Train Isolation Forest for anomaly detection.

        Args:
            training_data: DataFrame with features and labels
            feature_columns: List of feature column names

        Returns:
            Dictionary with model performance metrics
        """
        # Prepare features
        X = training_data[feature_columns].values
        y = training_data.get("anomaly_label", pd.Series([0] * len(X)))

        # Scale features
        X_scaled = self.scaler.fit_transform(X)

        # Train Isolation Forest
        self.isolation_forest.fit(X_scaled)

        # Make predictions
        anomaly_scores = self.isolation_forest.decision_function(X_scaled)
        predictions = self.isolation_forest.predict(X_scaled)

        # Calculate metrics
        from sklearn.metrics import (
            accuracy_score,
            f1_score,
            precision_score,
            recall_score,
            roc_auc_score,
        )

        metrics = {
            "accuracy": accuracy_score(y, predictions),
            "precision": precision_score(y, predictions, zero_division=0),
            "recall": recall_score(y, predictions, zero_division=0),
            "f1_score": f1_score(y, predictions, zero_division=0),
            "roc_auc": roc_auc_score(y, anomaly_scores) if len(set(y)) > 1 else 0.5,
            "anomaly_count": sum(predictions == -1),
            "normal_count": sum(predictions == 1),
            "n_features": len(feature_columns),
            "sample_size": len(X),
        }

        # Store scaler for inference
        self.scaler.fit(X)  # Refit for inference consistency

        return metrics

    def detect_anomalies_ml(
        self,
        events: list[AisEvent],
        feature_columns: list[str],
    ) -> list[dict[str, float]]:
        """Detect anomalies using trained Isolation Forest.

        Args:
            events: List of AIS events
            feature_columns: List of feature column names

        Returns:
            List of anomaly dictionaries with scores and explanations
        """
        if not hasattr(self.isolation_forest, 'fit'):
            return []  # Model not trained yet

        # Extract features for events
        feature_list = []
        for event in events:
            # Use feature store to get features
            if self.feature_store:
                features = self.feature_store.extract_temporal_features(
                    events, event
                )
            else:
                # Fallback: create basic features
                features = self._create_basic_features(event)

            # Filter to only include requested feature columns
            filtered_features = {
                col: features[col] for col in feature_columns
                if col in features
            }

            feature_list.append(filtered_features)

        # Convert to DataFrame
        df = pd.DataFrame(feature_list)

        # Ensure all feature columns are present
        for col in feature_columns:
            if col not in df.columns:
                df[col] = 0.0

        # Scale features
        X_scaled = self.scaler.transform(df[feature_columns])

        # Get anomaly scores
        anomaly_scores = self.isolation_forest.decision_function(X_scaled)
        predictions = self.isolation_forest.predict(X_scaled)

        # Create anomaly results
        anomalies = []
        for i, (event, score, prediction) in enumerate(
            zip(events, anomaly_scores, predictions, strict=False)
        ):
            if prediction == -1:  # Anomaly detected
                anomalies.append({
                    "event_id": event.event_id,
                    "mmsi": event.mmsi,
                    "event_time": event.event_time,
                    "anomaly_score": float(score),
                    "severity": self._calculate_ml_severity(score),
                    "explanation": f"Isolation Forest anomaly score: {score:.3f}",
                    "model_type": "isolation_forest",
                    "feature_values": df.iloc[i].to_dict(),
                })

        return anomalies

    def _calculate_ml_severity(self, anomaly_score: float) -> str:
        """Calculate severity based on anomaly score."""
        if anomaly_score < -0.5:
            return "high"
        elif anomaly_score < -0.2:
            return "medium"
        else:
            return "low"

    def _create_basic_features(self, event: AisEvent) -> dict[str, float]:
        """Create basic features for an event."""
        features = {
            "sog_knots": event.sog_knots or 0.0,
            "cog_degrees": event.cog_degrees or 0.0,
            "latitude": event.latitude,
            "longitude": event.longitude,
            "is_underway": 1 if event.nav_status == "Under way using engine" else 0,
            "hour_of_day": event.event_time.hour,
            "day_of_week": event.event_time.weekday(),
        }
        return features

    def train_route_clusterer(
        self,
        vessel_events: list[AisEvent],
    ) -> dict[str, float]:
        """Train DBSCAN for route discovery and clustering.

        Args:
            vessel_events: List of vessel events

        Returns:
            Dictionary with clustering metrics
        """
        # Extract spatial features for clustering
        features = []
        event_ids = []

        for event in vessel_events:
            # Extract spatial features
            feature_vector = self._extract_spatial_features(event, vessel_events)
            features.append(feature_vector)
            event_ids.append(event.event_id)

        if len(features) < self.route_clusterer.min_samples:
            return {
                "n_clusters": 0,
                "n_noise_points": len(features),
                "silhouette_score": None,
            }

        # Convert to numpy array
        X = np.array(features)

        # Apply DBSCAN clustering
        labels = self.route_clusterer.fit_predict(X)

        # Calculate metrics
        n_clusters = len(set(labels)) - (1 if -1 in labels else 0)
        n_noise_points = sum(1 for label in labels if label == -1)

        # Calculate silhouette score if we have clusters
        if n_clusters > 1 and n_noise_points < len(labels):
            from sklearn.metrics import silhouette_score
            silhouette = silhouette_score(X, labels)
        else:
            silhouette = None

        return {
            "n_clusters": n_clusters,
            "n_noise_points": n_noise_points,
            "silhouette_score": silhouette,
            "sample_size": len(features),
            "dbscan_eps": self.route_clusterer.eps,
            "dbscan_min_samples": self.route_clusterer.min_samples,
        }

    def discover_routes(
        self,
        vessel_events: list[AisEvent],
    ) -> dict[str, object]:
        """Discover vessel routes and cluster similar patterns.

        Args:
            vessel_events: List of vessel events

        Returns:
            Dictionary with route discovery results
        """
        if not hasattr(self.route_clusterer, 'fit'):
            return {"message": "Route clusterer not trained"}

        # Extract spatial features
        features = []
        event_ids = []
        vessel_mmsis = []

        for event in vessel_events:
            feature_vector = self._extract_spatial_features(event, vessel_events)
            features.append(feature_vector)
            event_ids.append(event.event_id)
            vessel_mmsis.append(event.mmsi)

        if len(features) < self.route_clusterer.min_samples:
            return {"message": "Insufficient data for route discovery"}

        # Apply clustering
        X = np.array(features)
        labels = self.route_clusterer.fit_predict(X)

        # Group events by cluster
        clusters = {}
        for i, label in enumerate(labels):
            if label == -1:
                cluster_key = "noise"
            else:
                cluster_key = f"cluster_{label}"

            if cluster_key not in clusters:
                clusters[cluster_key] = {
                    "events": [],
                    "event_ids": [],
                    "mmsi": vessel_mmsis[i],
                    "num_events": 0,
                }

            clusters[cluster_key]["events"].append(vessel_events[event_ids.index(event_ids[i])])
            clusters[cluster_key]["event_ids"].append(event_ids[i])
            clusters[cluster_key]["num_events"] += 1

        # Calculate cluster statistics
        route_results = {
            "clusters": {},
            "n_clusters": len([c for c in clusters.keys() if c != "noise"]),
            "n_noise_points": len(clusters.get("noise", {"events": []})["events"]),
            "model_type": "dbscan",
            "eps": self.route_clusterer.eps,
            "min_samples": self.route_clusterer.min_samples,
        }

        for cluster_key, cluster_data in clusters.items():
            if cluster_key == "noise":
                continue

            # Calculate route geometry
            events = cluster_data["events"]
            if len(events) >= 2:
                # Create line geometry
                lats = [e.latitude for e in events]
                lons = [e.longitude for e in events]

                # Calculate route statistics
                total_distance = self._calculate_route_distance(lats, lons)
                avg_speed = np.mean([e.sog_knots or 0 for e in events])
                avg_course = np.mean([e.cog_degrees or 0 for e in events if e.cog_degrees is not None])

                route_results["clusters"][cluster_key] = {
                    "mmsi": cluster_data["mmsi"],
                    "num_events": cluster_data["num_events"],
                    "total_distance_nm": total_distance,
                    "average_speed_knots": avg_speed,
                    "average_course_degrees": avg_course,
                    "start_time": min(e.event_time for e in events).isoformat(),
                    "end_time": max(e.event_time for e in events).isoformat(),
                    "confidence": self._calculate_route_confidence(events),
                }

        return route_results

    def _extract_spatial_features(
        self,
        event: AisEvent,
        all_events: list[AisEvent],
    ) -> list[float]:
        """Extract spatial features for route clustering."""
        # Find recent events for this vessel
        vessel_events = [e for e in all_events if e.mmsi == event.mmsi]
        if len(vessel_events) < 2:
            return [0.0] * 5  # Return zero vector if insufficient data

        # Calculate recent statistics
        recent_events = vessel_events[-5:] if len(vessel_events) >= 5 else vessel_events

        features = []

        # Position features
        features.extend([
            event.latitude,
            event.longitude,
            event.sog_knots or 0.0,
            event.cog_degrees or 0.0,
        ])

        # Velocity and course consistency
        speeds = [e.sog_knots or 0 for e in recent_events]
        courses = [e.cog_degrees or 0 for e in recent_events if e.cog_degrees is not None]

        features.extend([
            np.std(speeds) if len(speeds) > 1 else 0.0,
            np.mean(speeds) if speeds else 0.0,
            np.std(courses) if len(courses) > 1 else 0.0,
            np.mean(courses) if courses else 0.0,
        ])

        return features

    def _calculate_route_distance(self, lats: list[float], lons: list[float]) -> float:
        """Calculate total route distance using haversine formula."""
        if len(lats) < 2:
            return 0.0

        total_distance = 0.0
        for i in range(1, len(lats)):
            distance = self._haversine_distance(
                lats[i-1], lons[i-1], lats[i], lons[i]
            )
            total_distance += distance

        return total_distance

    def _calculate_route_confidence(self, events: list[AisEvent]) -> float:
        """Calculate confidence score for route detection."""
        if len(events) < 3:
            return 0.0

        # Calculate distances between consecutive points
        distances = []
        for i in range(1, len(events)):
            dist = self._haversine_distance(
                events[i-1].latitude, events[i-1].longitude,
                events[i].latitude, events[i].longitude
            )
            distances.append(dist)

        # Calculate speed consistency
        speeds = [e.sog_knots or 0 for e in events]
        speed_std = np.std(speeds) if len(speeds) > 1 else 0.0

        # Calculate course consistency
        courses = [e.cog_degrees or 0 for e in events if e.cog_degrees is not None]
        course_std = np.std(courses) if len(courses) > 1 else 0.0

        # Combine confidence factors
        distance_confidence = 1.0 - min(np.std(distances) / (np.mean(distances) if np.mean(distances) > 0 else 1), 1.0)
        speed_confidence = 1.0 - min(speed_std / (np.mean(speeds) if np.mean(speeds) > 0 else 1), 1.0)
        course_confidence = 1.0 - min(course_std / (np.mean(courses) if np.mean(courses) > 0 else 1), 1.0)

        confidence = (distance_confidence + speed_confidence + course_confidence) / 3.0

        return max(0.0, min(1.0, confidence))

    def _haversine_distance(self, lat1: float, lon1: float, lat2: float, lon2: float) -> float:
        """Calculate great-circle distance between two points in kilometers."""
        from math import atan2, cos, radians, sin, sqrt

        R = 6371  # Earth radius in km

        lat1, lon1, lat2, lon2 = map(radians, [lat1, lon1, lat2, lon2])

        dlat = lat2 - lat1
        dlon = lon2 - lon1

        a = sin(dlat / 2) ** 2 + cos(lat1) * cos(lat2) * sin(dlon / 2) ** 2
        c = 2 * atan2(sqrt(a), sqrt(1 - a))

        return R * c

    def train_ml_models(
        self,
        training_data: pd.DataFrame,
        feature_columns: list[str],
    ) -> dict[str, dict[str, float]]:
        """Train all ML models and return performance metrics."""
        results = {}

        # Train Isolation Forest
        iff_metrics = self.train_isolation_forest(training_data, feature_columns)
        results["isolation_forest"] = iff_metrics

        # Train route clusterer (simplified)
        results["route_clusterer"] = {
            "dbscan_eps": self.route_clusterer.eps,
            "dbscan_min_samples": self.route_clusterer.min_samples,
            "status": "ready" if hasattr(self.route_clusterer, 'fit') else "not_trained",
        }

        return results
