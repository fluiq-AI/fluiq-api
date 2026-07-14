"""fluiq-api — POST /api/v1/ingest/otel

Ingest OpenInference / OTLP spans from an external observability platform
(Arize Phoenix, Langfuse) or a Fluiq pull connector (LangSmith, Braintrust).
Each span is converted to a Fluiq event and pushed through the same pipeline as
native SDK traces: persisted to ClickHouse (via the trace topic) and fanned out
for per-call evaluation and security scanning. Run the agentic evaluator on the
whole run afterwards with the drawer's **Run Agentic Eval** button.

Auth: API key (same as ``/ingest``). Trace quota is a hard stop; eval quota only
gates the eval fan-out.
"""
from __future__ import annotations

import uuid
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel

import config
from db_queues.kafka import kafka_queue
from db_queues.postgresql.auth import resolve_api_key
from db_queues.postgresql.guardrails import get_policy
from routes.auth.helper import extract_api_key
from shared.quotas import bump_eval_count, bump_trace_count, get_quota_status
from .convert import to_events

otel_router = APIRouter()

_RETRIEVAL_APIS = {"query", "search", "near_text", "near_vector", "hybrid", "bm25", "query_points", "fetch_objects"}


class OtelIngestRequest(BaseModel):
    api_key: Optional[str] = None
    source: str = "openinference"        # "phoenix" | "langfuse" | "langsmith" | "braintrust" | ...
    # One of: OTLP-JSON (resourceSpans), flat spans, or pre-mapped events.
    resourceSpans: Optional[List[Dict[str, Any]]] = None
    spans: Optional[List[Dict[str, Any]]] = None
    events: Optional[List[Dict[str, Any]]] = None
    # Optional eval/security opt-in for the imported traces.
    eval_config: Optional[Dict[str, Any]] = None
    security_config: Optional[Dict[str, Any]] = None


class OtelIngestResponse(BaseModel):
    ok: bool
    source: str
    spans_received: int
    events_ingested: int
    root_trace_ids: List[str]


@otel_router.post("/ingest/otel", response_model=OtelIngestResponse)
async def ingest_otel(
    payload: OtelIngestRequest,
    api_key: Optional[str] = Depends(extract_api_key),
) -> OtelIngestResponse:
    api_key = api_key or payload.api_key
    if not api_key:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="API key required")
    resolved = await resolve_api_key(api_key)
    if resolved is None:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid API key")
    org_id, prefix, _key_id = resolved

    quota = await get_quota_status(org_id)
    if quota.trace_over:
        raise HTTPException(
            status_code=status.HTTP_402_PAYMENT_REQUIRED,
            detail=f"Trace quota exceeded for {quota.tier} tier. Upgrade to resume ingestion.",
        )

    raw = payload.model_dump(exclude_none=True)
    events, _root = to_events(raw, source=payload.source)
    if not events:
        raise HTTPException(status_code=422, detail="No spans/events could be converted")

    # Resolve the org's PII policy once if security scanning is requested.
    sec_policy = None
    if payload.security_config:
        sec_policy = await get_policy(org_id, slug=payload.security_config.get("guardrail", "default"))

    roots: set[str] = set()
    ingested = 0
    for event in events:
        trace_id = event.get("trace_id") or str(uuid.uuid4())
        event["trace_id"] = trace_id
        roots.add(event.get("root_trace_id") or trace_id)

        job = {
            "organization_id": str(org_id),
            "api_key_prefix": prefix,
            "trace_id": trace_id,
            "event": event,
            # Per-row retention window for this org's tier (Free=14d, paid=never).
            "retention_days": quota.retention_days,
        }
        # 1) persist the trace
        try:
            await kafka_queue.add_job(job, topic=config.KAFKA_TRACE_TOPIC, key=str(org_id))
        except Exception:
            continue
        bump_trace_count(org_id)
        ingested += 1

        if quota.eval_over:
            continue

        # 2) eval fan-out — retrieval → context precision; LLM → single-shot
        etype = event.get("type")
        if etype == "vectorstore" or (etype == "retrieval"):
            await kafka_queue.add_job(job, topic=config.KAFKA_EVAL_TOPIC, key=str(org_id))
            bump_eval_count(org_id)
        elif etype == "llm":
            _cfg = payload.eval_config or {"metrics": ["hallucination", "relevance"]}
            await kafka_queue.add_job(
                {**job, "eval_config": _cfg, "operation": "sdk_llm"},
                topic=config.KAFKA_EVAL_TOPIC, key=str(org_id),
            )
            bump_eval_count(org_id)

        # 3) security fan-out (opt-in)
        if payload.security_config and sec_policy is not None:
            await kafka_queue.add_job(
                {
                    **job,
                    "security_config": payload.security_config,
                    "operation": "sdk_security",
                    "pii_ignore": sec_policy.pii_ignore,
                    "allowed_tools": sec_policy.allowed_tools,
                },
                topic=config.KAFKA_SECURITY_TOPIC, key=str(org_id),
            )

    return OtelIngestResponse(
        ok=True, source=payload.source,
        spans_received=len(payload.spans or payload.resourceSpans or payload.events or []),
        events_ingested=ingested, root_trace_ids=sorted(roots),
    )


__all__ = ["otel_router"]
