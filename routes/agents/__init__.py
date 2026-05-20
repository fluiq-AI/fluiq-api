import uuid

from fastapi import APIRouter, Depends, HTTPException, Query, status

from db_queues.clickhouse import clickhouse_client
from db_queues.postgresql.auth import get_organization
from routes.auth.helper import get_current_session

from .model import AgentSummaryResponse, AgentSummaryRow

router = APIRouter()


@router.get("/agents/summary", response_model=AgentSummaryResponse)
async def agent_summary(
    session: dict = Depends(get_current_session),
    limit: int = Query(default=50, ge=1, le=1000),
    offset: int = Query(default=0, ge=0),
) -> AgentSummaryResponse:
    """Aggregate cost / token / latency metrics per agent across all history.

    The grouping key is the *root* trace's stable identifier — function name
    for ``@trace`` decorated entrypoints, chain name for LangChain roots,
    ``langgraph_node`` for LangGraph entries, and ``provider:model`` for raw
    LLM calls that have no wrapping decorator.
    """
    org_id = uuid.UUID(session["org_id"])
    organization = await get_organization(org_id)
    if organization is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Organization not found",
        )

    rows = await clickhouse_client.fetch_agent_summary(
        organization_id=org_id,
        limit=limit,
        offset=offset,
    )
    return AgentSummaryResponse(
        agents=[AgentSummaryRow(**row) for row in rows],
        limit=limit,
    )
