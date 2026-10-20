from datetime import datetime
from typing import Literal

from pydantic import BaseModel, Field, field_validator


class AisEvent(BaseModel):
    """Canonical, source-neutral AIS event exchanged on Kafka topics."""

    event_id: str = Field(min_length=8, max_length=200)
    source: Literal["replay", "aisstream", "marine_cadastre"]
    mmsi: int = Field(ge=100_000_000, le=999_999_999)
    event_time: datetime
    ingested_at: datetime
    latitude: float = Field(ge=-90, le=90)
    longitude: float = Field(ge=-180, le=180)
    sog_knots: float | None = Field(default=None, ge=0, le=102.2)
    cog_degrees: float | None = Field(default=None, ge=0, lt=360)
    heading_degrees: int | None = Field(default=None, ge=0, le=511)
    nav_status: str | None = Field(default=None, max_length=100)
    vessel_type: str | None = Field(default=None, max_length=100)

    @field_validator("event_time", "ingested_at")
    @classmethod
    def require_timezone(cls, value: datetime) -> datetime:
        if value.tzinfo is None:
            raise ValueError("timestamps must include a timezone")
        return value

