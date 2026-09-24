from datetime import datetime, timezone
from uuid import uuid4
from pydantic import BaseModel, Field

class EventEnvelope(BaseModel):
    event_id: str = Field(default_factory=lambda: str(uuid4()))
    event_type: str
    aggregate_id: str
    occurred_at: str = Field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    payload: dict
