"""Window metrics calculation for AIS events."""

from datetime import datetime, timedelta
from typing import Dict, List, Optional

from pydantic import BaseModel, Field

from ship_analytics.schemas import AisEvent
from ship_analytics.anomalies import Anomaly


class WindowMetrics(BaseModel):
    """Metrics calculated over a time window."""

    window_start: datetime
    window_end: datetime
    event_count: int = 0
    active_vessel_count: int = 0
    mean_speed_knots: float | None = None
    median_speed_knots: float | None = None
    vessel_type_counts: Dict[str, int] = Field(default_factory=dict)
    anomaly_count: int = 0


class WindowCalculator:
    """Calculate tumbling window metrics from AIS events."""

    def __init__(self, window_minutes: int = 60):
        self.window_minutes = window_minutes

    def calculate_metrics(
        self,
        events: List[AisEvent],
        window_start: datetime,
        window_end: datetime,
        anomalies: Optional[List[Anomaly]] = None,
    ) -> WindowMetrics:
        """Calculate all metrics for a given window."""
        window_events = [
            e for e in events
            if window_start <= e.event_time <= window_end
        ]

        if not window_events:
            return WindowMetrics(
                window_start=window_start,
                window_end=window_end,
                event_count=0,
                active_vessel_count=0,
                anomaly_count=len(anomalies) if anomalies else 0,
            )

        active_vessels = set(e.mmsi for e in window_events)
        speeds = [e.sog_knots for e in window_events if e.sog_knots is not None]

        vessel_type_counts: Dict[str, int] = {}
        for event in window_events:
            if event.vessel_type:
                vessel_type_counts[event.vessel_type] = (
                    vessel_type_counts.get(event.vessel_type, 0) + 1
                )

        mean_speed = sum(speeds) / len(speeds) if speeds else None
        sorted_speeds = sorted([s for s in speeds if s is not None])
        median_speed = (
            sorted_speeds[len(sorted_speeds) // 2]
            if sorted_speeds
            else None
        )

        anomaly_count = 0
        if anomalies:
            anomaly_count = sum(
                1 for anomaly in anomalies
                if window_start <= anomaly.event_time <= window_end
            )

        return WindowMetrics(
            window_start=window_start,
            window_end=window_end,
            event_count=len(window_events),
            active_vessel_count=len(active_vessels),
            mean_speed_knots=mean_speed,
            median_speed_knots=median_speed,
            vessel_type_counts=vessel_type_counts,
            anomaly_count=anomaly_count,
        )

    def tumbling_windows(
        self,
        events: List[AisEvent],
        anomalies: Optional[List[Anomaly]] = None,
    ) -> List[WindowMetrics]:
        """Generate tumbling windows for a series of events."""
        if not events:
            return []

        sorted_events = sorted(events, key=lambda e: e.event_time)
        min_time = sorted_events[0].event_time
        max_time = sorted_events[-1].event_time

        windows: List[WindowMetrics] = []
        window_start = datetime.fromtimestamp(
            (min_time.timestamp() // (self.window_minutes * 60)) * (self.window_minutes * 60),
            tz=min_time.tzinfo,
        )

        while window_start < max_time:
            window_end = window_start + timedelta(minutes=self.window_minutes)

            window_metrics = self.calculate_metrics(
                sorted_events, window_start, window_end, anomalies
            )
            windows.append(window_metrics)

            window_start = window_end

        return windows
