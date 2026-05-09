import asyncio
import json
import config
import uuid
from datetime import datetime, timezone
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query, Request, status
from sse_starlette.sse import EventSourceResponse

from db_queues.clickhouse import clickhouse_client
from db_queues.kafka import kafka_queue
from db_queues.postgresql.auth import get_organization, resolve_api_key
from realtime import running_registry, trace_broker
from routes.auth.helper import get_current_session
from shared.quotas import (
    bump_eval_count,
    bump_trace_count,
    get_quota_status,
)

from .model import IngestPayload, TraceListResponse, TraceRecord


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

    # ``status="running"`` is a live-progress signal emitted before the
    # actual call completes; the same trace_id will land again with the
    # final event. Don't bump quota here (would double-count) and skip
    # the evaluations fan-out (no inputs/outputs to score yet).
    is_running = event.get("status") == "running"

    job = {
        "organization_id": str(org_id),
        "api_key_prefix": prefix,
        "trace_id": trace_id,
        "event": event,
    }
    await kafka_queue.add_job(
        job,
        topic=config.KAFKA_TRACE_TOPIC,
        key=str(org_id),
    )
    if not is_running:
        bump_trace_count(org_id)

    eval_skipped = False
    if not is_running and _is_retrieval_event(event):
        if quota.eval_over:
            eval_skipped = True
        else:
            await kafka_queue.add_job(
                job,
                topic=config.KAFKA_EVAL_TOPIC,
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
    agent_key: Optional[str] = Query(default=None),
    agent_kind: Optional[str] = Query(default=None),
    root_trace_id: Optional[uuid.UUID] = Query(default=None),
    limit: int = Query(default=100, ge=1, le=1000),
    offset: int = Query(default=0, ge=0),
) -> TraceListResponse:
    """Return traces for the caller's organization.

    When `key_id` is omitted, traces from all of the org's API keys are
    returned. When provided, results are filtered to that key (404 if the
    key does not belong to the org). When `agent_key` and `agent_kind` are
    provided, only root traces for that agent are returned.
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
        agent_key=agent_key,
        agent_kind=agent_kind,
        root_trace_id=root_trace_id,
        limit=limit,
        offset=offset,
    )
    persisted = [TraceRecord(**row) for row in rows]
    # In-flight runs only surface on the first page; subsequent pages page
    # through historical (durable) rows where ephemeral entries don't
    # belong. The registry is scoped to this replica only — running rows
    # for connections that landed on a different replica won't appear
    # here, which is the same trade-off the SSE broker already makes.
    running: list[TraceRecord] = []
    if offset == 0:
        in_flight = await running_registry.list_for(
            organization_id=str(org_id),
            api_key_prefix=selected_prefix,
        )
        seen: set[str] = {
            str(p.event.get("trace_id"))
            for p in persisted
            if isinstance(p.event, dict) and p.event.get("trace_id")
        }
        for entry in in_flight:
            tid = entry["trace_id"]
            if tid in seen:
                continue
            ingested_ms = entry.get("ingested_at_ms")
            ingested_at = (
                datetime.fromtimestamp(ingested_ms / 1000.0, tz=timezone.utc)
                if isinstance(ingested_ms, (int, float))
                else datetime.now(tz=timezone.utc)
            )
            running.append(
                TraceRecord(
                    api_key_prefix=entry.get("api_key_prefix") or "",
                    event=entry.get("event") or {},
                    ingested_at=ingested_at,
                    cost=None,
                    currency=None,
                    evaluations=[],
                )
            )
    return TraceListResponse(
        traces=running + persisted,
        limit=limit,
        offset=offset,
    )


SSE_HEARTBEAT_SECONDS = 15.0


@router.get("/traces/stream")
async def stream_traces(
    request: Request,
    session: dict = Depends(get_current_session),
    key_id: Optional[uuid.UUID] = Query(default=None),
):
    """Server-Sent Events stream of newly persisted traces for the caller's
    org. Each event payload mirrors the TraceRecord shape (minus cost and
    evaluations, which arrive on later writes and surface on next refresh)
    so the frontend can prepend without re-fetching.
    """
    org_id = uuid.UUID(session["org_id"])
    organization = await get_organization(org_id)
    if organization is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Organization not found",
        )

    # Mirror the /ingest gate: orgs over their trace cap lose realtime
    # visibility too. The handshake fails fast (402) instead of opening a
    # long-lived connection that would never see new traces anyway.
    quota = await get_quota_status(org_id)
    if quota.trace_over:
        raise HTTPException(
            status_code=status.HTTP_402_PAYMENT_REQUIRED,
            detail=(
                f"Trace quota exceeded for {quota.tier} tier "
                f"({quota.trace_count}/{quota.trace_quota}). "
                f"Upgrade your plan to resume realtime streaming."
            ),
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

    async def event_source():
        # ``trace_broker.subscribe`` is an async context manager yielding
        # the underlying asyncio.Queue. We wrap ``queue.get()`` in
        # ``wait_for`` for heartbeats; cancelling that coroutine on
        # timeout doesn't damage the queue, so the loop keeps running for
        # the lifetime of the connection.
        async with trace_broker.subscribe(str(org_id)) as queue:
            yield {"event": "ready", "data": "{}"}
            while True:
                if await request.is_disconnected():
                    break
                try:
                    message = await asyncio.wait_for(
                        queue.get(),
                        timeout=SSE_HEARTBEAT_SECONDS,
                    )
                except asyncio.TimeoutError:
                    yield {"event": "ping", "data": "{}"}
                    continue
                if (
                    selected_prefix is not None
                    and isinstance(message, dict)
                    and message.get("api_key_prefix") != selected_prefix
                ):
                    continue
                # Route by ``kind`` so the frontend can prepend new rows
                # ("trace"), merge cost / evaluation updates by trace_id
                # ("trace.enriched"), or render an in-progress placeholder
                # ("trace.started") that the eventual "trace" event will
                # replace in place. Older producers without ``kind`` fall
                # back to the completed trace event.
                kind = message.get("kind") if isinstance(message, dict) else None
                if kind == "enriched":
                    event_name = "trace.enriched"
                elif kind == "started":
                    event_name = "trace.started"
                else:
                    event_name = "trace"
                yield {
                    "event": event_name,
                    "data": json.dumps(message, default=str),
                }

    return EventSourceResponse(event_source())