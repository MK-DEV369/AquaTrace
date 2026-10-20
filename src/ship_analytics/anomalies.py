from datetime import datetime, timedelta
from typing import List, Literal, TYPE_CHECKING

from pydantic import BaseModel, Field
from shapely.geometry import Point

from ship_analytics.schemas import AisEvent

if TYPE_CHECKING:
    from ship_analytics.windows import WindowMetrics


class Anomaly(BaseModel):
    """Anomaly record for detected vessel behavior issues."""

    model_config = {"arbitrary_types_allowed": True}

    mmsi: int = Field(ge=100_000_000, le=999_999_999)
    event_time: datetime
    anomaly_type: Literal[
        "invalid_jump", "unexpected_reporting_gap", "speed_threshold", "sharp_course_change"
    ]
    severity: Literal["low", "medium", "high"]
    explanation: str
    geom: Point
    evidence_json: dict[str, object] = Field(default_factory=dict)


class AnomalyDetector:
    """Detect anomalies in AIS vessel events based on domain rules."""

    INVALID_JUMP_SPEED_KNOTS = 60.0
    REPORTING_GAP_MINUTES = 30
    SPEED_THRESHOLDS = {
        "Cargo": 25.0,
        "Tanker": 20.0,
        "Passenger": 30.0,
        "Fishing": 15.0,
        "Default": 22.0,
    }
    COURSE_CHANGE_THRESHOLD = 90.0

    @staticmethod
    def detect_invalid_jump(event1: AisEvent, event2: AisEvent) -> Anomaly | None:
        """Detect when a vessel jumps too far in impossible time."""
        if event1.sog_knots is None or event2.sog_knots is None:
            return None

        time_diff = (event2.event_time - event1.event_time).total_seconds() / 3600
        if time_diff <= 0:
            return None

        distance = abs(event1.latitude - event2.latitude) + abs(event1.longitude - event2.longitude)
        implied_speed = distance / time_diff

        if implied_speed > AnomalyDetector.INVALID_JUMP_SPEED_KNOTS:
            return Anomaly(
                mmsi=event1.mmsi,
                event_time=event2.event_time,
                anomaly_type="invalid_jump",
                severity="high",
                explanation=f"Implied speed {implied_speed:.1f} knots exceeds {AnomalyDetector.INVALID_JUMP_SPEED_KNOTS} knot threshold",
                geom=Point(event2.longitude, event2.latitude),
                evidence_json={
                    "previous_event_id": event1.event_id,
                    "previous_sog": event1.sog_knots,
                    "time_diff_hours": time_diff,
                    "distance": distance,
                    "implied_speed": implied_speed,
                },
            )
        return None

    @staticmethod
    def detect_reporting_gap(current_event: AisEvent, last_event: AisEvent) -> Anomaly | None:
        """Detect when an underway vessel has no message for too long."""
        if current_event.nav_status != "Under way using engine":
            return None

        gap_minutes = (current_event.event_time - last_event.event_time).total_seconds() / 60

        if gap_minutes > AnomalyDetector.REPORTING_GAP_MINUTES:
            return Anomaly(
                mmsi=current_event.mmsi,
                event_time=current_event.event_time,
                anomaly_type="unexpected_reporting_gap",
                severity="medium",
                explanation=f"Vessel has no message for {gap_minutes:.0f} minutes exceeding {AnomalyDetector.REPORTING_GAP_MINUTES} minute threshold",
                geom=Point(current_event.longitude, current_event.latitude),
                evidence_json={
                    "previous_event_id": last_event.event_id,
                    "gap_minutes": gap_minutes,
                    "vessel_type": current_event.vessel_type,
                    "last_sog": last_event.sog_knots,
                },
            )
        return None

    @staticmethod
    def detect_speed_threshold(event: AisEvent) -> Anomaly | None:
        """Detect when vessel speed exceeds class-specific thresholds."""
        if event.sog_knots is None:
            return None

        threshold = AnomalyDetector.SPEED_THRESHOLDS.get(event.vessel_type, AnomalyDetector.SPEED_THRESHOLDS["Default"])

        if event.sog_knots > threshold:
            return Anomaly(
                mmsi=event.mmsi,
                event_time=event.event_time,
                anomaly_type="speed_threshold",
                severity="medium",
                explanation=f"Vessel speed {event.sog_knots} knots exceeds {event.vessel_type or 'Default'} threshold of {threshold} knots",
                geom=Point(event.longitude, event.latitude),
                evidence_json={
                    "sog": event.sog_knots,
                    "threshold": threshold,
                    "vessel_type": event.vessel_type,
                },
            )
        return None

    @staticmethod
    def detect_sharp_course_change(current_event: AisEvent, previous_event: AisEvent) -> Anomaly | None:
        """Detect sharp course changes within short intervals."""
        if current_event.cog_degrees is None or previous_event.cog_degrees is None:
            return None

        course_change = abs(current_event.cog_degrees - previous_event.cog_degrees)
        if course_change > 180:
            course_change = 360 - course_change

        if course_change > AnomalyDetector.COURSE_CHANGE_THRESHOLD:
            return Anomaly(
                mmsi=current_event.mmsi,
                event_time=current_event.event_time,
                anomaly_type="sharp_course_change",
                severity="low",
                explanation=f"Course changed by {course_change:.1f} degrees within short interval",
                geom=Point(current_event.longitude, current_event.latitude),
                evidence_json={
                    "previous_cog": previous_event.cog_degrees,
                    "current_cog": current_event.cog_degrees,
                    "course_change": course_change,
                },
            )
        return None

    @classmethod
    def detect_anomalies(
        cls,
        current_event: AisEvent,
        previous_event: AisEvent | None = None,
        last_known_state: AisEvent | None = None,
    ) -> list[Anomaly]:
        """Run all anomaly detection rules on a vessel event."""
        anomalies = []

        if previous_event:
            anomalies.append(cls.detect_invalid_jump(previous_event, current_event))
            anomalies.append(cls.detect_sharp_course_change(current_event, previous_event))

        if last_known_state:
            anomalies.append(cls.detect_reporting_gap(current_event, last_known_state))

        anomalies.append(cls.detect_speed_threshold(current_event))

        return [a for a in anomalies if a is not None]