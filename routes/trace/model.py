from datetime import datetime
from pydantic import BaseModel
from typing import Any

class IngestPayload(BaseModel):
    api_key: str | None = None
    event: dict[str, Any]


class TraceRecord(BaseModel):
    api_key_prefix: str
    event: dict[str, Any]
    ingested_at: datetime


class TraceListResponse(BaseModel):
    traces: list[TraceRecord]
    limit: int
    offset: int