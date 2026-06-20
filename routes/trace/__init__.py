import asyncio
import json
import config
import uuid
from datetime import datetime, timezone
from typing import Optional

from aiokafka.errors import MessageSizeTooLargeError
from fastapi import APIRouter, Depends, HTTPException, Query, Request, status
from sse_starlette.sse import EventSourceResponse

from db_queues.clickhouse import clickhouse_client
from db_queues.kafka import kafka_queue, wait_for_reply
from db_queues.postgresql.auth import get_organization, resolve_api_key
from db_queues.postgresql.guardrails import get_policy
from realtime import running_registry, trace_broker
from routes.auth.helper import extract_api_key, get_current_session
from shared.quotas import (
    bump_eval_count,
    bump_trace_count,
    get_quota_status,
)

from .model import (
    IngestPayload,
    SpendingResponse,
    SpendingDay,
    TraceListResponse,
    TraceRecord,
)


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
async def ingestion(
    payload: IngestPayload,
    api_key: Optional[str] = Depends(extract_api_key),
):
    api_key = api_key or payload.api_key
    if not api_key:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="API key required",
        )
    resolved = await resolve_api_key(api_key)
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

    # Strip SDK-embedded configs before persisting the trace.
    eval_config     = event.pop("_eval_config",     None)
    security_config = event.pop("_security_config", None)

    is_running = event.get("status") == "running"

    # ── Response gate ──────────────────────────────────────────────────────────
    # Runs BEFORE Kafka publish so the result is embedded in the stored event.
    # When scan_responses=True for this org, scan the response synchronously and
    # stamp event["response_gate_blocked"]=True if flagged — this reaches ClickHouse.
    response_gated = False
    ingest_extra: dict = {}

    if not is_running and security_config and event.get("type") == "llm":
        _resp = event.get("response") or event.get("output") or ""
        if isinstance(_resp, list):
            _resp = " ".join(str(x) for x in _resp if x)
        elif not isinstance(_resp, str):
            _resp = str(_resp) if _resp else ""

        if _resp:
            _guardrail = security_config.get("guardrail", "default")
            policy = await get_policy(org_id, slug=_guardrail)
            if policy.scan_responses:
                correlation_id = str(uuid.uuid4())
                try:
                    await kafka_queue.add_job(
                        {
                            "operation":      "response_gate_check",
                            "response":       _resp,
                            "correlation_id": correlation_id,
                            "pii_ignore":     policy.pii_ignore,
                        },
                        topic=config.KAFKA_SECURITY_TOPIC,
                    )
                    gate = await wait_for_reply(correlation_id, timeout=3.0)
                    if gate and gate.get("response_blocked"):
                        event["response_gate_blocked"] = True  # persisted in ClickHouse
                        ingest_extra = {
                            "response_blocked": True,
                            "block_reason":     gate.get("block_reason"),
                            "risk_level":       gate.get("risk_level", "high"),
                            "attack_types":     gate.get("attack_types", []),
                        }
                        response_gated = True
                except Exception:
                    pass  # fail open

    # ── Kafka publish ──────────────────────────────────────────────────────────
    job = {
        "organization_id": str(org_id),
        "api_key_prefix":  prefix,
        "trace_id":        trace_id,
        "event":           event,
    }
    # A trace event larger than the producer's max_request_size used to crash
    # here with an unhandled MessageSizeTooLargeError → 500. Return an explicit
    # 413 instead so the SDK/client gets an actionable error and we don't log a
    # traceback for an oversized-payload condition. Guarding the trace publish is
    # enough: the eval/security fan-outs below reuse the same `event`, so a
    # rejected trace short-circuits before they run.
    try:
        await kafka_queue.add_job(job, topic=config.KAFKA_TRACE_TOPIC, key=str(org_id))
    except MessageSizeTooLargeError as exc:
        raise HTTPException(
            status_code=status.HTTP_413_REQUEST_ENTITY_TOO_LARGE,
            detail=(
                "Trace event exceeds the maximum ingestion size. Reduce the size "
                "of prompts, responses, or tool outputs captured in this event."
            ),
        ) from exc
    if not is_running:
        bump_trace_count(org_id)

    eval_skipped = False
    if not is_running and _is_retrieval_event(event):
        if quota.eval_over:
            eval_skipped = True
        else:
            await kafka_queue.add_job(job, topic=config.KAFKA_EVAL_TOPIC, key=str(org_id))
            bump_eval_count(org_id)
    elif not is_running and eval_config and event.get("type") == "llm":
        if quota.eval_over:
            eval_skipped = True
        else:
            await kafka_queue.add_job(
                {**job, "eval_config": eval_config, "operation": "sdk_llm"},
                topic=config.KAFKA_EVAL_TOPIC,
                key=str(org_id),
            )
            bump_eval_count(org_id)

    if not is_running and security_config:
        # Forward the org's warn-mode PII policy so the worker suppresses ignored
        # entity types (cached get_policy — cheap; reuses the response-gate fetch).
        _sec_policy = await get_policy(org_id, slug=security_config.get("guardrail", "default"))
        security_job = {
            **job,
            "security_config": security_config,
            "operation":       "sdk_security",
            "pii_ignore":      _sec_policy.pii_ignore,
            "allowed_tools":   _sec_policy.allowed_tools,
        }
        if response_gated:
            security_job["response_gated"] = True
        await kafka_queue.add_job(security_job, topic=config.KAFKA_SECURITY_TOPIC, key=str(org_id))

    return {"ok": True, "trace_id": trace_id, "eval_skipped": eval_skipped, **ingest_extra}


@router.get("/traces", response_model=TraceListResponse)
async def list_traces(
    session: dict = Depends(get_current_session),
    key_id: Optional[uuid.UUID] = Query(default=None),
    agent_key: Optional[str] = Query(default=None),
    agent_kind: Optional[str] = Query(default=None),
    root_trace_id: Optional[uuid.UUID] = Query(default=None),
    roots_only: bool = Query(default=False),
    limit: int = Query(default=100, ge=1, le=1000),
    offset: int = Query(default=0, ge=0),
    sort: str = Query(default="newest"),
    status: str = Query(default="all"),
    security: str = Query(default="all"),
    integration: str = Query(default="all"),
    quality: str = Query(default="all"),
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
        roots_only=roots_only,
        limit=limit,
        offset=offset,
        sort=sort,
        status=status,
        security=security,
        integration=integration,
        quality=quality,
    )
    persisted = [TraceRecord(**row) for row in rows]
    # In-flight runs only surface on the first page; subsequent pages page
    # through historical (durable) rows where ephemeral entries don't
    # belong. The registry is scoped to this replica only — running rows
    # for connections that landed on a different replica won't appear
    # here, which is the same trade-off the SSE broker already makes.
    running: list[TraceRecord] = []
    if offset == 0 and status in ("all", "running"):
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
            # When roots_only is requested, skip in-flight spans (those
            # whose root_trace_id differs from their own trace_id).
            if roots_only:
                rtid = entry.get("event", {}).get("root_trace_id")
                if rtid and rtid != tid:
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


# Provider name (as stored in trace_costs) → spending-chart bucket.
_SPEND_PROVIDER_BUCKET = {
    "openai": "openai",
    "anthropic": "anthropic",
    "google": "google",
}


@router.get("/traces/spending", response_model=SpendingResponse)
async def trace_spending(
    session: dict = Depends(get_current_session),
    days: int = Query(default=30, ge=1, le=365),
) -> SpendingResponse:
    """Daily spend by provider for the dashboard spending chart.

    Server-side aggregation so the chart doesn't pull ~1000 fully-joined trace
    rows on first paint (see fetch_spending_by_day).
    """
    org_id = uuid.UUID(session["org_id"])
    rows = await clickhouse_client.fetch_spending_by_day(org_id, days=days)

    by_date: dict[str, SpendingDay] = {}
    for row in rows:
        day = row["day"]
        if not day:
            continue
        bucket = _SPEND_PROVIDER_BUCKET.get((row["provider"] or "").lower(), "other")
        cost = row["cost"]
        entry = by_date.get(day)
        if entry is None:
            entry = SpendingDay(date=day)
            by_date[day] = entry
        setattr(entry, bucket, getattr(entry, bucket) + cost)
        entry.all += cost

    return SpendingResponse(days=sorted(by_date.values(), key=lambda d: d.date))


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