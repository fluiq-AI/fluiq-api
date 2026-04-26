import os
import uuid
from typing import Optional

from dotenv import load_dotenv
from fastapi import APIRouter, Depends, HTTPException, Query, status

from db_queues.clickhouse import clickhouse_client
from db_queues.kafka import kafka_queue
from db_queues.postgresql.auth import get_organization, resolve_api_key
from routes.auth.helper import get_current_session

from .model import IngestPayload, TraceListResponse, TraceRecord

load_dotenv()

router = APIRouter()

@router.post("/ingest")
async def ingestion(payload: IngestPayload):
    if not payload.api_key:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="API key required",
        )
    resolved = await resolve_api_key(payload.api_key)
    if resolved is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid API key",
        )
    org_id, prefix, _key_id = resolved

    job = {
        "organization_id": str(org_id),
        "api_key_prefix": prefix,
        "event": payload.event,
    }
    await kafka_queue.add_job(
        job,
        topic=os.getenv("KAFKA_TRACE_TOPIC"),
        key=str(org_id),
    )

    return {"ok": True}


@router.get("/traces", response_model=TraceListResponse)
async def list_traces(
    session: dict = Depends(get_current_session),
    key_id: Optional[uuid.UUID] = Query(default=None),
    limit: int = Query(default=100, ge=1, le=1000),
    offset: int = Query(default=0, ge=0),
) -> TraceListResponse:
    """Return traces for the caller's organization.

    When `key_id` is omitted, traces from all of the org's API keys are
    returned. When provided, results are filtered to that key (404 if the
    key does not belong to the org).
    """
    org_id = uuid.UUID(session["org_id"])
    organization = await get_organization(org_id)
    if organization is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Organization not found",
        )

    selected_prefix: Optional[str] = None
    if key_id is not None:
        match = next(
            (k for k in (organization.api_keys or []) if k.key_id == key_id),
            None,
        )
        if match is None:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="API key not found",
            )
        selected_prefix = match.prefix

    rows = await clickhouse_client.fetch_traces(
        organization_id=org_id,
        api_key_prefix=selected_prefix,
        limit=limit,
        offset=offset,
    )
    return TraceListResponse(
        traces=[TraceRecord(**row) for row in rows],
        limit=limit,
        offset=offset,
    )