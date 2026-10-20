#!/usr/bin/env python3
"""Project verification script for Phase 1 implementation."""

import os
import sys
from datetime import UTC, datetime, timedelta


def check_file_exists(path, description):
    """Check if a file exists."""
    if os.path.exists(path):
        print(f"[OK] {description}: {path}")
        return True
    else:
        print(f"[FAIL] {description}: {path}")
        return False

def check_imports():
    """Check if key modules can be imported."""
    modules = [
        "ship_analytics.schemas",
        "ship_analytics.anomalies",
        "ship_analytics.windows",
        "ship_analytics.models",
    ]

    print("\nChecking imports:")
    all_ok = True
    for module in modules:
        try:
            __import__(module)
            print(f"[OK] {module}")
        except ImportError as e:
            print(f"[FAIL] {module}: {e}")
            all_ok = False

    return all_ok

def test_anomaly_detection():
    """Test anomaly detection functionality."""
    print("\nTesting anomaly detection:")
    try:
        from ship_analytics.anomalies import AnomalyDetector
        from ship_analytics.schemas import AisEvent

        detector = AnomalyDetector()
        now = datetime.now(UTC)

        # Create test events with large enough distance to trigger invalid jump
        event1 = AisEvent(
            event_id="event_000001",
            source="replay",
            mmsi=123456789,
            event_time=now,
            ingested_at=now,
            latitude=1.0,
            longitude=103.0,
            sog_knots=10.0,
        )

        event2 = AisEvent(
            event_id="event_000002",
            source="replay",
            mmsi=123456789,
            event_time=now + timedelta(hours=1),
            ingested_at=now + timedelta(hours=1, seconds=1),
            latitude=89.0,  # Large jump (88nm lat + 77nm lon = 165nm in 1hr = 165 knots > 60 threshold)
            longitude=180.0,
            sog_knots=10.0,
        )

        anomalies = detector.detect_anomalies(event2, event1)

        if anomalies and len(anomalies) > 0:
            print(f"[OK] Anomaly detection working - detected {len(anomalies)} anomaly(s)")
            return True
        else:
            print("[FAIL] Anomaly detection failed - no anomalies detected")
            return False

    except Exception as e:
        print(f"[FAIL] Anomaly detection test failed: {e}")
        return False

def test_window_calculation():
    """Test window calculation functionality."""
    print("\nTesting window calculation:")
    try:
        from ship_analytics.schemas import AisEvent
        from ship_analytics.windows import WindowCalculator

        calculator = WindowCalculator(window_minutes=60)
        now = datetime.now(UTC)

        events = []
        for i in range(3):
            event = AisEvent(
                event_id=f"event_{i:06d}",
                source="replay",
                mmsi=100000000 + i,
                event_time=now + timedelta(minutes=i * 10),
                ingested_at=now + timedelta(minutes=i * 10, seconds=1),
                latitude=1.0 + i * 0.01,
                longitude=103.0 + i * 0.01,
                sog_knots=10.0 + i,
                vessel_type="Cargo" if i % 2 == 0 else "Tanker",
            )
            events.append(event)

        windows = calculator.tumbling_windows(events, [])

        if windows and len(windows) > 0:
            print(f"[OK] Window calculation working - generated {len(windows)} window(s)")
            return True
        else:
            print("[FAIL] Window calculation failed - no windows generated")
            return False

    except Exception as e:
        print(f"[FAIL] Window calculation test failed: {e}")
        return False

def main():
    """Main verification function."""
    print("=" * 60)
    print("Ship Analytics Phase 1 Implementation Verification")
    print("=" * 60)

    # Check project structure
    print("\nChecking project structure:")
    core_files = [
        ("src/ship_analytics/schemas.py", "AIS Event Schema Definition"),
        ("src/ship_analytics/anomalies.py", "Anomaly Detection"),
        ("src/ship_analytics/windows.py", "Window Metrics Calculator"),
        ("src/ship_analytics/models.py", "Database Models"),
        ("src/ship_analytics/config.py", "Configuration"),
        ("src/ship_analytics/kafka.py", "Kafka Integration"),
        ("services/api/main.py", "FastAPI API"),
        ("services/stream_processor/job.py", "Spark Stream Processor"),
        ("services/producer/replay.py", "Replay Producer"),
        ("services/producer/aisstream.py", "AISStream Producer"),
        ("services/dashboard/app.py", "Streamlit Dashboard"),
        ("tests/test_schemas.py", "Basic Tests"),
        ("tests/test_phase1.py", "Phase 1 Comprehensive Tests"),
        ("db/init/002_schema.sql", "Database Schema"),
        ("db/migrations/003_anomalies_and_windows.sql", "Anomaly & Window Schema"),
        ("pyproject.toml", "Project Configuration"),
        ("docker-compose.yml", "Docker Configuration"),
        (".env.example", "Environment Template"),
    ]

    all_files_exist = True
    for file_path, description in core_files:
        if not check_file_exists(file_path, description):
            all_files_exist = False

    # Check imports
    imports_ok = check_imports()

    # Test functionality
    anomaly_ok = test_anomaly_detection()
    window_ok = test_window_calculation()

    # Summary
    print("\n" + "=" * 60)
    print("VERIFICATION SUMMARY")
    print("=" * 60)

    files_status = "PASS" if all_files_exist else "FAIL"
    imports_status = "PASS" if imports_ok else "FAIL"
    functionality_status = "PASS" if (anomaly_ok and window_ok) else "FAIL"

    print(f"Project Structure: {files_status}")
    print(f"Module Imports: {imports_status}")
    print(f"Functionality Tests: {functionality_status}")

    if all_files_exist and imports_ok and (anomaly_ok and window_ok):
        print("\nALL CHECKS PASSED - Phase 1 implementation is complete!")
        return 0
    else:
        print("\nSOME CHECKS FAILED - Please review the implementation")
        return 1

if __name__ == "__main__":
    sys.exit(main())