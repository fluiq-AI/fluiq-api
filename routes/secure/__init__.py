"""fluiq-api  —  POST /api/v1/secure/check

Lightweight pre-call guard.  Publishes the prompt to the evaluator worker via
Kafka (priority path) for a full scan (PII + secrets + semantic + patterns).
Falls back to fast pattern-only matching if the worker doesn't reply within
KAFKA_SECURITY_CHECK_TIMEOUT seconds.

Requires Team tier or above.
"""
from __future__ import annotations

import logging
from typing import List, Optional
from uuid import uuid4

from fastapi import APIRouter, HTTPException, status
from pydantic import BaseModel

import config
from db_queues.postgresql.auth import resolve_api_key, get_org_tier
from db_queues.kafka import kafka_queue, wait_for_reply
from . import scanners

logger = logging.getLogger(__name__)
router = APIRouter()

_SECURE_TIERS = {"Team", "Growth", "Enterprise"}


async def _resolve_and_gate(api_key: str) -> tuple:
    if not api_key:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="API key required")
    resolved = await resolve_api_key(api_key)
    if resolved is None:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid API key")
    org_id, prefix, _key_id = resolved
    tier = await get_org_tier(org_id) or "Free"
    if tier not in _SECURE_TIERS:
        raise HTTPException(
            status_code=status.HTTP_402_PAYMENT_REQUIRED,
            detail=(
                f"fluiq.secure() requires Team plan or above "
                f"(current plan: {tier}). Upgrade at getfluiq.com/dashboard."
            ),
        )
    return org_id, prefix


class CheckRequest(BaseModel):
    api_key:  str
    prompt:   str
    trace_id: Optional[str] = None
    context:  Optional[dict] = None


class CheckResponse(BaseModel):
    allow:        bool
    block_reason: Optional[str]
    risk_level:   str
    attack_types: List[str]


async def _publish_blocked_trace(
    org_id: str,
    prefix: str,
    trace_id: str,
    result: CheckResponse,
    context: Optional[dict] = None,
) -> None:
    try:
        event: dict = {
            "trace_id":     trace_id,
            "type":         "llm",
            "status":       "blocked",
            "success":      False,
            "output":       result.block_reason,
            "block_reason": result.block_reason,
            "risk_level":   result.risk_level,
            "attack_types": result.attack_types,
        }
        if context:
            event.update({k: v for k, v in context.items() if v is not None})
        await kafka_queue.add_job(
            {
                "organization_id": str(org_id),
                "api_key_prefix":  prefix,
                "trace_id":        trace_id,
                "event":           event,
            },
            topic=config.KAFKA_TRACE_TOPIC,
            key=str(org_id),
        )
    except Exception:
        logger.exception("[SECURE] Failed to publish blocked trace trace_id=%s", trace_id)


@router.post("/secure/check", response_model=CheckResponse)
async def pre_call_check(payload: CheckRequest) -> CheckResponse:
    """Pre-call security guard.

    Sends the prompt to the evaluator worker for a full scan (PII, secrets,
    semantic classifier, attack patterns).  If the worker replies within the
    configured timeout the full result is returned.  On timeout the endpoint
    falls back to the fast pattern-only check so the LLM call is never blocked
    indefinitely by a slow or unavailable worker.

    When the result is a block and the SDK supplied a ``trace_id``, publishes a
    ``status="blocked"`` trace event so the in-flight "running" placeholder is
    replaced on the dashboard.
    """
    org_id, prefix = await _resolve_and_gate(payload.api_key)

    # Priority path: full scan via the evaluator worker
    correlation_id = uuid4().hex
    try:
        await kafka_queue.add_job(
            {
                "operation":      "security_check_sync",
                "prompt":         payload.prompt,
                "correlation_id": correlation_id,
            },
            topic=config.KAFKA_EVAL_TOPIC,
        )
        result = await wait_for_reply(correlation_id, timeout=config.KAFKA_SECURITY_CHECK_TIMEOUT)
        if result is not None:
            response = CheckResponse(**result)
            if not response.allow and payload.trace_id:
                await _publish_blocked_trace(str(org_id), prefix, payload.trace_id, response, payload.context)
            return response
    except Exception:
        logger.exception("[SECURE] Kafka full-scan failed, falling back to pattern check")

    # Fallback: fast pattern-only check (no PII / secrets / semantic)
    logger.warning("[SECURE] Falling back to pattern-only check for correlation_id=%s", correlation_id)
    r = scanners.check(payload.prompt)
    response = CheckResponse(
        allow        = r.allow,
        block_reason = r.block_reason,
        risk_level   = r.risk_level,
        attack_types = r.attack_types,
    )
    if not response.allow and payload.trace_id:
        await _publish_blocked_trace(str(org_id), prefix, payload.trace_id, response, payload.context)
    return response
