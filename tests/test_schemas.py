from datetime import UTC, datetime

import pytest
from pydantic import ValidationError

from ship_analytics.schemas import AisEvent


def valid_event() -> dict[str, object]:
    now = datetime.now(UTC)
    return {
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


def test_valid_event_is_accepted() -> None:
    assert AisEvent.model_validate(valid_event()).mmsi == 123456789


def test_invalid_coordinates_are_rejected() -> None:
    event = valid_event()
    event["latitude"] = 91.0
    with pytest.raises(ValidationError):
        AisEvent.model_validate(event)

