from datetime import datetime, UTC
from typing import Any
from pydantic import BaseModel, Field, field_validator
import uuid


class Event(BaseModel):
    topic: str = Field(..., min_length=1, description="Nama topic/channel event")
    event_id: str = Field(
        default_factory=lambda: str(uuid.uuid4()),
        description="ID unik event (UUID v4 direkomendasikan)"
    )
    timestamp: datetime = Field(
        default_factory=lambda: datetime.now(UTC),
        description="Waktu event dalam format ISO8601"
    )
    source: str = Field(..., min_length=1, description="Sumber/publisher event")
    payload: dict[str, Any] = Field(default_factory=dict, description="Data event bebas")

    @field_validator("topic")
    @classmethod
    def topic_no_spaces(cls, v: str) -> str:
        if " " in v:
            raise ValueError("Topic tidak boleh mengandung spasi; gunakan underscore atau titik.")
        return v.lower()

    @field_validator("event_id")
    @classmethod
    def event_id_not_empty(cls, v: str) -> str:
        if not v.strip():
            raise ValueError("event_id tidak boleh kosong.")
        return v.strip()

    model_config = {"ser_json_timedelta": "iso8601"}


class PublishRequest(BaseModel):
    events: list[Event]


class EventResponse(BaseModel):
    topic: str
    event_id: str
    timestamp: str
    source: str
    payload: dict[str, Any]
    processed_at: str


class StatsResponse(BaseModel):
    received: int
    unique_processed: int
    duplicate_dropped: int
    topics: list[str]
    uptime_seconds: float
