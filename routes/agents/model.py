from datetime import datetime
from pydantic import BaseModel


class AgentSummaryRow(BaseModel):
    agent_key: str
    agent_kind: str
    integration: str
    runs: int
    total_cost: float
    avg_cost_per_run: float
    total_tokens: int
    avg_latency: float | None = None
    last_run: datetime | None = None


class AgentSummaryResponse(BaseModel):
    agents: list[AgentSummaryRow]
    limit: int
