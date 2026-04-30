import os
import uuid
from typing import Optional

from dotenv import load_dotenv
from fastapi import APIRouter, Depends, HTTPException, Query, status

from db_queues.clickhouse import clickhouse_client
from db_queues.kafka import kafka_queue
from db_queues.postgresql.auth import get_organization, resolve_api_key
from routes.auth.helper import get_current_session
from shared.quotas import (
    bump_eval_count,
    bump_trace_count,
    get_quota_status,
)

from .model import IngestPayload, TraceListResponse, TraceRecord

load_dotenv()

router = APIRouter()

_RETRIEVAL_APIS = {
    "query",
    "search",
    "near_text",
    "near_vector",
    "hybrid",
    "bm25",
    "query_points",
    "fetch_objects",
}


def _is_retrieval_event(event: dict) -> bool:
    return (
        event.get("type") == "vectorstore"
        and event.get("api") in _RETRIEVAL_APIS
    )


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

    # Enforce tier quotas before doing any Kafka work. Trace quota is a hard
    # stop (402); eval quota gates only the evaluations fan-out so tracing
    # keeps flowing even after the eval cap is reached.
    quota = await get_quota_status(org_id)
    if quota.trace_over:
        raise HTTPException(
            status_code=status.HTTP_402_PAYMENT_REQUIRED,
            detail=(
                f"Trace quota exceeded for {quota.tier} tier "
                f"({quota.trace_count}/{quota.trace_quota}). "
                f"Upgrade your plan to resume ingestion."
            ),
        )

    event = payload.event
    trace_id = event.get("trace_id")
    if not trace_id:
        trace_id = str(uuid.uuid4())
        event["trace_id"] = trace_id

    job = {
        "organization_id": str(org_id),
        "api_key_prefix": prefix,
        "trace_id": trace_id,
        "event": event,
    }
    await kafka_queue.add_job(
        job,
        topic=os.getenv("KAFKA_TRACE_TOPIC", "traces"),
        key=str(org_id),
    )
    bump_trace_count(org_id)

    eval_skipped = False
    if _is_retrieval_event(event):
        if quota.eval_over:
            eval_skipped = True
        else:
            await kafka_queue.add_job(
                job,
                topic=os.getenv("KAFKA_EVAL_TOPIC", "evaluations"),
                key=str(org_id),
            )
            bump_eval_count(org_id)

    return {
        "ok": True,
        "trace_id": trace_id,
        "eval_skipped": eval_skipped,
    }


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