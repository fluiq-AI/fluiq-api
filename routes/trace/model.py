from datetime import datetime
from pydantic import BaseModel
from typing import Any

class IngestPayload(BaseModel):
    api_key: str | None = None
    event: dict[str, Any]


class EvaluationScore(BaseModel):
    metric: str
    score: float | None = None
    evaluator: str = ""
    judge_model: str = ""
    details: dict[str, Any] | None = None


class TraceRecord(BaseModel):
    api_key_prefix: str
    event: dict[str, Any]
    ingested_at: datetime
    cost: float | None = None
    currency: str | None = None
    evaluations: list[EvaluationScore] = []
    # Labels, from the SDK at call time or the dashboard afterwards. Populated
    # per page rather than per row — see list_traces.
    tags: list[str] = []


class TraceListResponse(BaseModel):
    traces: list[TraceRecord]
    limit: int
    offset: int


class SpendingDay(BaseModel):
    date: str          # YYYY-MM-DD (UTC)
    all: float = 0.0
    openai: float = 0.0
    anthropic: float = 0.0
    google: float = 0.0
    other: float = 0.0


class SpendingResponse(BaseModel):
    days: list[SpendingDay]