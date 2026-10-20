"""Feature store implementation for Phase 2: Materialize vessel behavior features for ML."""

from datetime import datetime, timedelta

import numpy as np
import pandas as pd
from sqlalchemy import create_engine, text

from ship_analytics.config import get_settings
from ship_analytics.schemas import AisEvent


class FeatureStore:
    """Materialize vessel behavior features for ML training and inference."""

    def __init__(self):
        self.settings = get_settings()
        self.engine = create_engine(self.settings.database_url)

    def extract_temporal_features(
        self,
        all_events: list[AisEvent],
        target_event: AisEvent,
        window_minutes: int = 60,
    ) -> dict[str, float]:
        """Extract temporal features for a target event.

        Args:
            all_events: All vessel events in chronological order
            target_event: The event to generate features for
            window_minutes: Time window for lag features

        Returns:
            Dictionary of feature names and values
        """
        vessel_events = [
            e for e in all_events if e.mmsi == target_event.mmsi
        ]
        vessel_events.sort(key=lambda e: e.event_time)

        target_index = next(
            i for i, e in enumerate(vessel_events) if e.event_time == target_event.event_time
        )

        features = {}

        # Speed and course features
        if target_index > 0:
            prev_event = vessel_events[target_index - 1]
            features["speed_change_rate"] = (
                (target_event.sog_knots or 0) - (prev_event.sog_knots or 0)
            )
            features["course_change_rate"] = (
                (target_event.cog_degrees or 0) - (prev_event.cog_degrees or 0)
            )

            # Time-weighted average speed
            time_weighted_speed = self._calculate_time_weighted_speed(
                vessel_events, target_index, window_minutes
            )
            features["time_weighted_avg_speed"] = time_weighted_speed

        # Recent statistics
        recent_events = vessel_events[max(0, target_index - 10):target_index]
        if recent_events:
            speeds = [e.sog_knots or 0 for e in recent_events]
            features["recent_speed_std"] = np.std(speeds) if len(speeds) > 1 else 0.0
            features["recent_speed_mean"] = np.mean(speeds)

            courses = [e.cog_degrees or 0 for e in recent_events if e.cog_degrees is not None]
            if courses:
                features["recent_course_std"] = np.std(courses)

        # Reporting gap
        if target_index > 0:
            prev_event = vessel_events[target_index - 1]
            gap_minutes = (target_event.event_time - prev_event.event_time).total_seconds() / 60
            features["reporting_gap_minutes"] = gap_minutes
            features["is_reporting_gap_violation"] = gap_minutes > 30

        # Vessel type encoding
        vessel_type_map = {
            "Cargo": 0,
            "Tanker": 1,
            "Passenger": 2,
            "Fishing": 3,
            "default": 4,
        }
        features["vessel_type_encoded"] = vessel_type_map.get(
            target_event.vessel_type, vessel_type_map["default"]
        )

        # Navigation status
        nav_status_map = {
            "Under way using engine": 1,
            "Moored": 0,
            "Anchored": 0,
            "anchored": 0,
            "default": 2,
        }
        features["nav_status_encoded"] = nav_status_map.get(
            target_event.nav_status, nav_status_map["default"]
        )

        # Time-based features
        hour = target_event.event_time.hour
        features["hour_of_day"] = hour
        features["is_peak_hour"] = 1 if 7 <= hour <= 19 else 0

        return features

    def _calculate_time_weighted_speed(
        self,
        events: list[AisEvent],
        target_index: int,
        window_minutes: int,
    ) -> float:
        """Calculate time-weighted average speed over a window."""
        window_start = events[target_index].event_time - timedelta(
            minutes=window_minutes
        )

        window_events = [
            e for e in events
            if e.event_time >= window_start and e.event_time <= events[target_index].event_time
        ]

        if not window_events:
            return 0.0

        total_distance = 0.0
        total_time = 0.0

        for i in range(1, len(window_events)):
            prev = window_events[i - 1]
            curr = window_events[i]

            time_diff = (curr.event_time - prev.event_time).total_seconds() / 3600
            if time_diff > 0:
                distance = self._haversine_distance(
                    prev.latitude, prev.longitude,
                    curr.latitude, curr.longitude
                )
                speed = distance / time_diff
                total_distance += distance
                total_time += time_diff * speed

        return total_distance / total_time if total_time > 0 else 0.0

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

    def store_features(self, vessel_mmsi: int, features: dict[str, float]) -> None:
        """Store features in the feature store (Parquet + PostGIS)."""
        # Store to Parquet format
        df = pd.DataFrame([features])
        df.to_parquet(
            f"features/vessel_{vessel_mmsi}_features.parquet",
            index=False,
        )

        # Store spatial features to PostGIS
        self._store_spatial_features(vessel_mmsi, features)

    def _store_spatial_features(self, vessel_mmsi: int, features: dict[str, float]) -> None:
        """Store spatial features to PostGIS."""
        with self.engine.connect() as conn:
            # This would store vessel position and features
            conn.execute(
                text("")
            )  # TODO: Implement spatial feature storage

    def get_features_for_training(
        self,
        start_time: datetime,
        end_time: datetime,
        target_feature: str = "anomaly_label",
    ) -> pd.DataFrame:
        """Retrieve features for ML training."""
        query = text("""
            SELECT f.*, v.mmsi, v.event_time, v.latitude, v.longitude,
                   CASE WHEN f.anomaly_label IS NULL THEN 0 ELSE 1 END as target
            FROM features f
            JOIN ais_event_clean v ON f.event_id = v.event_id
            WHERE v.event_time BETWEEN :start_time AND :end_time
        """)

        with self.engine.connect() as conn:
            df = pd.read_sql(
                query, conn, params={"start_time": start_time, "end_time": end_time}
            )

        return df

    def generate_windowed_features(
        self,
        events: list[AisEvent],
        window_minutes: int = 60,
    ) -> list[dict[str, float]]:
        """Generate windowed features for all events."""
        features_list = []

        for i, event in enumerate(events):
            # Generate features for event at window boundary
            if i % (window_minutes // 5) == 0:  # 5-minute sub-windows
                window_start = event.event_time - timedelta(
                    minutes=window_minutes
                )
                window_events = [
                    e for e in events
                    if window_start <= e.event_time <= event.event_time
                ]

                if len(window_events) >= 2:
                    features = self._aggregate_window_features(window_events)
                    features["event_time"] = event.event_time
                    features["mmsi"] = event.mmsi
                    features_list.append(features)

        return features_list

    def _aggregate_window_features(
        self,
        events: list[AisEvent],
    ) -> dict[str, float]:
        """Aggregate features over a time window."""
        if not events:
            return {}

        features = {}

        # Speed statistics
        speeds = [e.sog_knots or 0 for e in events]
        features["window_speed_mean"] = np.mean(speeds)
        features["window_speed_std"] = np.std(speeds) if len(speeds) > 1 else 0.0
        features["window_speed_max"] = np.max(speeds)
        features["window_speed_min"] = np.min(speeds)

        # Course statistics
        courses = [e.cog_degrees or 0 for e in events if e.cog_degrees is not None]
        if courses:
            features["window_course_mean"] = np.mean(courses)
            features["window_course_std"] = np.std(courses)

        # Reporting gap analysis
        gaps = []
        for i in range(1, len(events)):
            gap = (events[i].event_time - events[i - 1].event_time).total_seconds() / 60
            gaps.append(gap)

        if gaps:
            features["window_reporting_gap_mean"] = np.mean(gaps)
            features["window_reporting_gap_max"] = np.max(gaps)
            features["window_reporting_gap_violations"] = sum(1 for gap in gaps if gap > 30)

        # Vessel activity
        features["window_event_count"] = len(events)
        features["window_vessel_count"] = len(set(e.mmsi for e in events))

        return features

    def load_training_data(
        self,
        start_time: datetime,
        end_time: datetime,
    ) -> pd.DataFrame:
        """Load training data with features and labels."""
        # Get events in time range
        query = text("""
            SELECT event_id, mmsi, event_time, latitude, longitude,
                   sog_knots, cog_degrees, vessel_type, nav_status
            FROM ais_event_clean
            WHERE event_time BETWEEN :start_time AND :end_time
            ORDER BY mmsi, event_time
        """)

        with self.engine.connect() as conn:
            events_df = pd.read_sql(
                query, conn, params={"start_time": start_time, "end_time": end_time}
            )

        # Generate features for each event
        all_features = []
        for _, row in events_df.iterrows():
            event = AisEvent(
                event_id=row["event_id"],
                source="replay",  # Default for training
                mmsi=row["mmsi"],
                event_time=row["event_time"],
                ingested_at=row["event_time"],  # Use event_time as ingested_at for training
                latitude=row["latitude"],
                longitude=row["longitude"],
                sog_knots=row["sog_knots"],
                cog_degrees=row["cog_degrees"],
                heading_degrees=None,
                nav_status=row["nav_status"],
                vessel_type=row["vessel_type"],
            )

            # Get all events for this vessel
            vessel_events = events_df[
                (events_df["mmsi"] == row["mmsi"]) & 
                (events_df["event_time"] <= row["event_time"])
            ].sort_values("event_time")

            # Convert to AisEvent objects
            ais_events = []
            for _, event_row in vessel_events.iterrows():
                ais_event = AisEvent(
                    event_id=event_row["event_id"],
                    source="replay",
                    mmsi=event_row["mmsi"],
                    event_time=event_row["event_time"],
                    ingested_at=event_row["event_time"],
                    latitude=event_row["latitude"],
                    longitude=event_row["longitude"],
                    sog_knots=event_row["sog_knots"],
                    cog_degrees=event_row["cog_degrees"],
                    heading_degrees=None,
                    nav_status=event_row["nav_status"],
                    vessel_type=event_row["vessel_type"],
                )
                ais_events.append(ais_event)

            # Generate features
            features = self.extract_temporal_features(ais_events, event)
            features["event_id"] = event.event_id
            features["event_time"] = event.event_time
            features["mmsi"] = event.mmsi

            # Add anomaly label if available
            try:
                from ship_analytics.anomalies import AnomalyDetector
                detector = AnomalyDetector()

                # Check if this event is part of an anomaly
                prev_event = ais_events[ais_events.index(event) - 1] if ais_events.index(event) > 0 else None
                last_known_state = ais_events[ais_events.index(event) - 2] if ais_events.index(event) > 1 else None

                anomalies = detector.detect_anomalies(event, prev_event, last_known_state)
                features["anomaly_label"] = 1 if anomalies else 0
                features["anomaly_types"] = ";".join([a.anomaly_type for a in anomalies]) if anomalies else ""
            except ImportError:
                features["anomaly_label"] = 0
                features["anomaly_types"] = ""

            all_features.append(features)

        return pd.DataFrame(all_features)
