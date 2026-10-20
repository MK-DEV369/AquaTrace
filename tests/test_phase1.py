"""Comprehensive tests for Phase 1 implementation."""

from datetime import UTC, datetime, timedelta

import pytest

from ship_analytics.anomalies import AnomalyDetector
from ship_analytics.config import get_settings
from ship_analytics.kafka import ensure_topics
from ship_analytics.schemas import AisEvent
from ship_analytics.windows import WindowCalculator


class TestAisEventSchema:
    """Tests for AIS event schema validation."""

    def test_valid_event(self):
        """Test that valid AIS events are accepted."""
        now = datetime.now(UTC)
        event_data = {
            "event_id": "replay:123456789:2026-10-19T09:00:00Z:1",
            "source": "replay",
            "mmsi": 123456789,
            "event_time": now,
            "ingested_at": now,
            "latitude": 1.28,
            "longitude": 103.84,
            "sog_knots": 12.5,
            "cog_degrees": 90.0,
        }
        event = AisEvent(**event_data)
        assert event.mmsi == 123456789
        assert event.latitude == 1.28

    def test_invalid_mmsi_rejected(self):
        """Test that events with invalid MMSI are rejected."""
        now = datetime.now(UTC)
        event_data = {
            "event_id": "replay:123456789:2026-10-19T09:00:00Z:1",
            "source": "replay",
            "mmsi": 99999,  # Too small
            "event_time": now,
            "ingested_at": now,
            "latitude": 1.28,
            "longitude": 103.84,
        }

        with pytest.raises(ValueError):
            AisEvent(**event_data)

    def test_invalid_latitude_rejected(self):
        """Test that events with invalid latitude are rejected."""
        now = datetime.now(UTC)
        event_data = {
            "event_id": "replay:123456789:2026-10-19T09:00:00Z:1",
            "source": "replay",
            "mmsi": 123456789,
            "event_time": now,
            "ingested_at": now,
            "latitude": 100.0,  # Invalid latitude
            "longitude": 103.84,
        }

        with pytest.raises(ValueError):
            AisEvent(**event_data)

    def test_timezone_required(self):
        """Test that timestamps without timezone are rejected."""
        now = datetime.now()  # No timezone
        event_data = {
            "event_id": "replay:123456789:2026-10-19T09:00:00Z:1",
            "source": "replay",
            "mmsi": 123456789,
            "event_time": now,
            "ingested_at": now,
            "latitude": 1.28,
            "longitude": 103.84,
        }

        with pytest.raises(ValueError):
            AisEvent(**event_data)

    def test_canonical_event_id(self):
        """Test event ID format validation."""
        now = datetime.now(UTC)
        event_data = {
            "event_id": "test:123:2026-01-01T00:00:00Z:suffix",  # Valid format
            "source": "replay",
            "mmsi": 123456789,
            "event_time": now,
            "ingested_at": now,
            "latitude": 1.28,
            "longitude": 103.84,
        }
        event = AisEvent(**event_data)
        assert "test:" in event.event_id
        assert "123" in event.event_id
        assert "2026-01-01" in event.event_id


class TestAnomalyDetector:
    """Tests for anomaly detection logic."""

    def setup_method(self):
        """Set up test fixtures."""
        self.detector = AnomalyDetector()

    def test_invalid_jump_detection(self):
        """Test detection of invalid jump anomalies."""
        now = datetime.now(UTC)
        previous = AisEvent(
            event_id="event_000001",
            source="replay",
            mmsi=123456789,
            event_time=now,
            ingested_at=now,
            latitude=1.0,
            longitude=103.0,
            sog_knots=10.0,
        )

        current = AisEvent(
            event_id="event_000002",
            source="replay",
            mmsi=123456789,
            event_time=now + timedelta(hours=1),
            ingested_at=now + timedelta(hours=1, seconds=1),
            latitude=89.0,  # Large latitude jump (88nm)
            longitude=180.0,  # Large longitude jump (77nm)
            sog_knots=10.0,
        )

        anomalies = self.detector.detect_anomalies(current, previous_event=previous)
        assert len(anomalies) == 1
        assert anomalies[0].anomaly_type == "invalid_jump"
        assert anomalies[0].severity == "high"

    def test_no_invalid_jump_for_small_movement(self):
        """Test that small movements don't trigger invalid jump."""
        now = datetime.now(UTC)
        previous = AisEvent(
            event_id="event_000001",
            source="replay",
            mmsi=123456789,
            event_time=now,
            ingested_at=now,
            latitude=1.0,
            longitude=103.0,
            sog_knots=10.0,
        )

        current = AisEvent(
            event_id="event_000002",
            source="replay",
            mmsi=123456789,
            event_time=now + timedelta(minutes=1),
            ingested_at=now + timedelta(minutes=1, seconds=1),
            latitude=1.1,  # Small movement
            longitude=103.1,  # Small movement
            sog_knots=10.0,
        )

        anomalies = self.detector.detect_anomalies(current, previous)
        assert len(anomalies) == 0

    def test_reporting_gap_detection(self):
        """Test detection of reporting gap anomalies."""
        now = datetime.now(UTC)
        previous = AisEvent(
            event_id="event_000001",
            source="replay",
            mmsi=123456789,
            event_time=now,
            ingested_at=now,
            latitude=1.0,
            longitude=103.0,
            sog_knots=10.0,
            nav_status="Under way using engine",
        )

        current = AisEvent(
            event_id="event_000002",
            source="replay",
            mmsi=123456789,
            event_time=now + timedelta(hours=1),  # 1 hour gap
            ingested_at=now + timedelta(hours=1, seconds=1),
            latitude=1.1,
            longitude=103.1,
            sog_knots=10.0,
            nav_status="Under way using engine",
        )

        anomalies = self.detector.detect_anomalies(current, last_known_state=previous)
        assert len(anomalies) == 1
        assert anomalies[0].anomaly_type == "unexpected_reporting_gap"

    def test_no_reporting_gap_for_non_underway(self):
        """Test that non-underway vessels don't trigger reporting gap."""
        now = datetime.now(UTC)
        previous = AisEvent(
            event_id="event_000001",
            source="replay",
            mmsi=123456789,
            event_time=now,
            ingested_at=now,
            latitude=1.0,
            longitude=103.0,
            sog_knots=0.0,
            nav_status="Moored",
        )

        current = AisEvent(
            event_id="event_000002",
            source="replay",
            mmsi=123456789,
            event_time=now + timedelta(hours=2),  # Large gap
            ingested_at=now + timedelta(hours=2, seconds=1),
            latitude=1.1,
            longitude=103.1,
            sog_knots=0.0,
            nav_status="Moored",
        )

        anomalies = self.detector.detect_anomalies(current, previous_event=previous)
        assert len(anomalies) == 0

    def test_speed_threshold_detection(self):
        """Test detection of speed threshold anomalies."""
        now = datetime.now(UTC)
        event = AisEvent(
            event_id="event_000001",
            source="replay",
            mmsi=123456789,
            event_time=now,
            ingested_at=now,
            latitude=1.0,
            longitude=103.0,
            sog_knots=30.0,  # Exceeds Cargo threshold of 25.0
            vessel_type="Cargo",
        )

        anomalies = self.detector.detect_anomalies(event)
        assert len(anomalies) == 1
        assert anomalies[0].anomaly_type == "speed_threshold"
        assert anomalies[0].severity == "medium"

    def test_sharp_course_change_detection(self):
        """Test detection of sharp course change anomalies."""
        now = datetime.now(UTC)
        previous = AisEvent(
            event_id="event_000001",
            source="replay",
            mmsi=123456789,
            event_time=now,
            ingested_at=now,
            latitude=1.0,
            longitude=103.0,
            cog_degrees=0.0,
        )

        current = AisEvent(
            event_id="event_000002",
            source="replay",
            mmsi=123456789,
            event_time=now + timedelta(minutes=1),
            ingested_at=now + timedelta(minutes=1, seconds=1),
            latitude=1.1,
            longitude=103.1,
            cog_degrees=180.0,  # Sharp change from 0 to 180
        )

        anomalies = self.detector.detect_anomalies(current, previous_event=previous)
        assert len(anomalies) == 1
        assert anomalies[0].anomaly_type == "sharp_course_change"
        assert anomalies[0].severity == "low"

    def test_anomaly_severity_levels(self):
        """Test that different anomalies have appropriate severity levels."""
        now = datetime.now(UTC)

        # Invalid jump should be high severity
        previous = AisEvent(
            event_id="event_000001",
            source="replay",
            mmsi=123456789,
            event_time=now,
            ingested_at=now,
            latitude=1.0,
            longitude=103.0,
            sog_knots=10.0,
        )

        current = AisEvent(
            event_id="event_000002",
            source="replay",
            mmsi=123456789,
            event_time=now + timedelta(hours=1),
            ingested_at=now + timedelta(hours=1, seconds=1),
            latitude=89.0,  # Large latitude jump (88nm)
            longitude=180.0,  # Large longitude jump (77nm)
            sog_knots=10.0,
        )

        anomalies = self.detector.detect_anomalies(current, previous_event=previous)
        assert anomalies[0].severity == "high"

        # Speed threshold should be medium severity
        event = AisEvent(
            event_id="event_000003",
            source="replay",
            mmsi=123456789,
            event_time=now,
            ingested_at=now,
            latitude=1.0,
            longitude=103.0,
            sog_knots=30.0,
            vessel_type="Cargo",
        )

        anomalies2 = self.detector.detect_anomalies(event)
        assert anomalies2[0].severity == "medium"

        # Sharp course change should be low severity
        previous2 = AisEvent(
            event_id="event_000004",
            source="replay",
            mmsi=123456789,
            event_time=now,
            ingested_at=now,
            latitude=1.0,
            longitude=103.0,
            cog_degrees=0.0,
        )

        current2 = AisEvent(
            event_id="event_000005",
            source="replay",
            mmsi=123456789,
            event_time=now + timedelta(minutes=1),
            ingested_at=now + timedelta(minutes=1, seconds=1),
            latitude=1.1,
            longitude=103.1,
            cog_degrees=180.0,
        )

        anomalies3 = self.detector.detect_anomalies(current2, previous_event=previous2)
        assert anomalies3[0].severity == "low"


class TestWindowCalculator:
    """Tests for window calculator functionality."""

    def setup_method(self):
        """Set up test fixtures."""
        self.calculator = WindowCalculator(window_minutes=60)

    def test_empty_window(self):
        """Test calculation for window with no events."""
        now = datetime.now(UTC)
        events = []
        anomalies = []

        window = self.calculator.calculate_metrics(events, now, now + timedelta(hours=1), anomalies)

        assert window.event_count == 0
        assert window.active_vessel_count == 0
        assert window.mean_speed_knots is None
        assert window.median_speed_knots is None

    def test_window_with_single_event(self):
        """Test calculation for window with single event."""
        now = datetime.now(UTC)
        event = AisEvent(
            event_id="event_000001",
            source="replay",
            mmsi=123456789,
            event_time=now,
            ingested_at=now,
            latitude=1.0,
            longitude=103.0,
            sog_knots=15.0,
            vessel_type="Cargo",
        )
        events = [event]

        window = self.calculator.calculate_metrics(events, now, now + timedelta(hours=1), [])

        assert window.event_count == 1
        assert window.active_vessel_count == 1
        assert window.mean_speed_knots == 15.0
        assert window.median_speed_knots == 15.0
        assert window.vessel_type_counts == {"Cargo": 1}

    def test_multiple_vessels_window(self):
        """Test calculation for window with multiple vessels."""
        now = datetime.now(UTC)
        event1 = AisEvent(
            event_id="event_000001",
            source="replay",
            mmsi=123456789,
            event_time=now,
            ingested_at=now,
            latitude=1.0,
            longitude=103.0,
            sog_knots=15.0,
            vessel_type="Cargo",
        )
        event2 = AisEvent(
            event_id="event_000002",
            source="replay",
            mmsi=987654321,
            event_time=now + timedelta(minutes=30),
            ingested_at=now + timedelta(minutes=30, seconds=1),
            latitude=1.1,
            longitude=103.1,
            sog_knots=20.0,
            vessel_type="Tanker",
        )
        events = [event1, event2]

        window = self.calculator.calculate_metrics(events, now, now + timedelta(hours=1), [])

        assert window.event_count == 2
        assert window.active_vessel_count == 2
        assert window.mean_speed_knots == 17.5
        assert window.vessel_type_counts == {"Cargo": 1, "Tanker": 1}

    def test_window_with_anomalies(self):
        """Test calculation for window with anomalies."""
        now = datetime.now(UTC)
        event = AisEvent(
            event_id="event_000001",
            source="replay",
            mmsi=123456789,
            event_time=now,
            ingested_at=now,
            latitude=1.0,
            longitude=103.0,
            sog_knots=15.0,
        )
        events = [event]

        # Create a mock anomaly
        class MockAnomaly:
            def __init__(self, event_time, mmsi):
                self.event_time = event_time
                self.mmsi = mmsi
                self.anomaly_type = "test"
                self.severity = "high"
                self.explanation = "Test anomaly"
                self.geom = None

        anomaly = MockAnomaly(now, 123456789)

        window = self.calculator.calculate_metrics(events, now, now + timedelta(hours=1), [anomaly])

        assert window.event_count == 1
        assert window.active_vessel_count == 1
        assert window.anomaly_count == 1

    def test_tumbling_windows(self):
        """Test generation of tumbling windows."""
        now = datetime.now(UTC)
        events = []
        for i in range(10):
            event = AisEvent(
                event_id=f"event_{i:06d}",
                source="replay",
                mmsi=100000000 + i,
                event_time=now + timedelta(minutes=i * 10),
                ingested_at=now + timedelta(minutes=i * 10, seconds=1),
                latitude=1.0 + i * 0.01,
                longitude=103.0 + i * 0.01,
                sog_knots=10.0 + i,
            )
            events.append(event)

        windows = self.calculator.tumbling_windows(events, [])

        assert len(windows) > 0
        assert all(w.window_start < w.window_end for w in windows)
        assert all(w.window_end - w.window_start == timedelta(minutes=60) for w in windows)

    def test_window_boundaries(self):
        """Test that events are correctly assigned to windows."""
        now = datetime.now(UTC)
        event1 = AisEvent(
            event_id="event_000001",
            source="replay",
            mmsi=123456789,
            event_time=now,  # In first window
            ingested_at=now,
            latitude=1.0,
            longitude=103.0,
            sog_knots=10.0,
        )
        event2 = AisEvent(
            event_id="event_000002",
            source="replay",
            mmsi=123456789,
            event_time=now + timedelta(minutes=61),  # In second window
            ingested_at=now + timedelta(minutes=61, seconds=1),
            latitude=1.1,
            longitude=103.1,
            sog_knots=10.0,
        )
        events = [event1, event2]

        windows = self.calculator.tumbling_windows(events, [])

        window1 = next(w for w in windows if w.window_start <= event1.event_time < w.window_end)
        window2 = next(w for w in windows if w.window_start <= event2.event_time < w.window_end)

        assert window1.event_count == 1
        assert window2.event_count == 1
        assert window1.window_start < window2.window_start


class TestIntegration:
    """Integration tests for the full pipeline."""

    def test_event_processing_pipeline(self):
        """Test the full event processing pipeline."""
        import sys
        sys.path.insert(0, "services")
        from stream_processor.job import AISProcessor

        processor = AISProcessor()

        # Create mock events
        now = datetime.now(UTC)
        events = []
        for i in range(5):
            event = AisEvent(
                event_id=f"event_{i:06d}",
                source="replay",
                mmsi=100000000 + i,
                event_time=now + timedelta(minutes=i * 5),
                ingested_at=now + timedelta(minutes=i * 5, seconds=1),
                latitude=1.0 + i * 0.01,
                longitude=103.0 + i * 0.01,
                sog_knots=10.0 + i,
                vessel_type="Cargo" if i % 2 == 0 else "Tanker",
            )
            events.append(event)

        # Process events
        anomalies = processor.detect_and_categorize_anomalies(events)
        windows = processor.calculate_window_metrics(events)

        assert isinstance(anomalies, list)
        assert isinstance(windows, list)

    @pytest.mark.skip(reason="Requires Docker/Kafka broker to be running")
    def test_kafka_topic_creation(self):
        """Test that Kafka topics are created correctly."""
        settings = get_settings()
        ensure_topics(settings.kafka_brokers)

        # This test verifies that the function doesn't raise an exception
        # Topics should already exist or be created successfully


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
