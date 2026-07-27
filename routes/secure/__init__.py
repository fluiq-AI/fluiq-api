"""fluiq-api  —  POST /api/v1/secure/check

Lightweight pre-call guard.  Publishes the prompt to the evaluator worker via
Kafka (priority path) for a full scan (PII + secrets + semantic + patterns).
Falls back to fast pattern-only matching if the worker doesn't reply within
KAFKA_SECURITY_CHECK_TIMEOUT seconds.

The org's GuardrailPolicy is fetched (60 s cache) and:
  - custom_allow_list  → prompt is immediately allowed if it matches
  - custom_deny_list   → prompt is immediately blocked before any scanner runs
  - block_categories   → sent to the worker; only listed attack types block
  - block_threshold    → sent to the worker; 'medium' lowers the block bar
  - alert_webhook      → POSTed asynchronously on every block

Metered by scan volume per plan (see ``shared.quotas.TIER_SECURITY_QUOTAS``);
available on every plan including Free.
"""
from __future__ import annotations

import asyncio
import logging
from typing import List, Optional
from uuid import uuid4

import httpx
from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel, Field

import config
from db_queues.postgresql.auth import resolve_api_key
from shared.quotas import get_quota_status
from db_queues.postgresql.guardrails import get_policy, GuardrailPolicy
from db_queues.kafka import kafka_queue, wait_for_reply
from routes.auth.helper import extract_api_key
from shared.net import is_safe_public_url
from shared.ids import coerce_trace_uuid
from . import scanners

logger = logging.getLogger(__name__)
router = APIRouter()

async def _resolve_and_gate(api_key: str) -> tuple:
    """Resolve the caller and check their security-scan allowance.

    Metered by volume rather than gated by tier. Scanning costs us regex and
    NER time, not tokens, so locking it behind the second-most-expensive plan
    priced the cheapest thing we run as if it were the most expensive — and
    meant nobody could try the one pillar our competitors do not ship at all.
    Every plan including Free gets a real allowance; volume is what you pay for.
    """
    if not api_key:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="API key required")
    resolved = await resolve_api_key(api_key)
    if resolved is None:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid API key")
    org_id, prefix, _key_id = resolved

    status_ = await get_quota_status(org_id)
    if status_.security_over:
        raise HTTPException(
            status_code=status.HTTP_402_PAYMENT_REQUIRED,
            detail=(
                f"Monthly security-scan allowance used "
                f"({status_.security_count:,} / {status_.security_quota:,} on "
                f"{status_.tier}). Upgrade at getfluiq.com/dashboard."
            ),
        )
    return org_id, prefix


# Upper bound on scanned prompt size. Prevents a multi-MB prompt from driving
# unbounded regex/preprocessing work per request (DoS). Larger prompts are
# truncated for scanning; the caller's actual LLM call is unaffected.
_MAX_PROMPT_CHARS = 64 * 1024


class CheckRequest(BaseModel):
    api_key:   Optional[str] = None
    prompt:    str = Field(max_length=1_000_000)
    trace_id:  Optional[str] = None
    context:   Optional[dict] = None
    guardrail: str = "default"


class CheckResponse(BaseModel):
    allow:        bool
    block_reason: Optional[str]
    risk_level:   str
    attack_types: List[str]
    # True when the verdict came from the degraded pattern-only fallback (the full
    # worker scan — PII/secrets/semantic — did not run). Lets callers fail closed.
    degraded:     bool = False


# ── Webhook ───────────────────────────────────────────────────────────────────

async def _fire_webhook(
    webhook_url: str,
    org_id: str,
    trace_id: Optional[str],
    risk_level: str,
    attack_types: List[str],
) -> None:
    # Re-validate at fire time (defends against a policy stored before the SSRF
    # guard existed, and narrows the DNS-rebinding window). Never follow
    # redirects — a redirect could hop to an internal host.
    if not await is_safe_public_url(webhook_url, require_https=True):
        logger.warning("[SECURE] Refusing to POST alert to non-public webhook")
        return
    payload = {
        "event":        "security.block",
        "org_id":       org_id,
        "trace_id":     trace_id,
        "risk_level":   risk_level,
        "attack_types": attack_types,
    }
    for attempt in range(3):
        try:
            async with httpx.AsyncClient(timeout=5.0, follow_redirects=False) as client:
                r = await client.post(webhook_url, json=payload)
                if r.status_code < 500:
                    return
        except Exception:
            pass
        await asyncio.sleep(2 ** attempt)
    logger.warning("[SECURE] Webhook delivery failed after 3 attempts: %s", webhook_url)


def _maybe_fire_webhook(
    policy: GuardrailPolicy,
    org_id: str,
    trace_id: Optional[str],
    risk_level: str,
    attack_types: List[str],
) -> None:
    if not policy.alert_webhook:
        return
    if risk_level not in policy.alert_on:
        return
    asyncio.create_task(_fire_webhook(
        policy.alert_webhook, org_id, trace_id, risk_level, attack_types,
    ))


# ── Policy helpers ────────────────────────────────────────────────────────────

def _apply_allow_list(prompt: str, policy: GuardrailPolicy) -> bool:
    """Return True if the prompt matches any allow-list phrase (case-insensitive)."""
    pl = prompt.lower()
    return any(phrase.lower() in pl for phrase in policy.custom_allow_list if phrase)


def _apply_deny_list(prompt: str, policy: GuardrailPolicy) -> Optional[str]:
    """Return a block reason if the prompt matches any deny-list phrase, else None."""
    pl = prompt.lower()
    for phrase in policy.custom_deny_list:
        if phrase and phrase.lower() in pl:
            return f"Blocked by custom deny list: matched '{phrase}'"
    return None


def _filter_by_categories(attack_types: List[str], policy: GuardrailPolicy) -> List[str]:
    """If block_categories is configured, only those categories cause a block."""
    if not policy.block_categories:
        return attack_types
    return [t for t in attack_types if t in policy.block_categories]


# ── Kafka helpers ─────────────────────────────────────────────────────────────

async def _publish_blocked_trace(
    org_id: str,
    prefix: str,
    trace_id: str,
    result: CheckResponse,
    context: Optional[dict] = None,
) -> None:
    try:
        # Merge caller-supplied context FIRST so the reserved security fields
        # below always win. Otherwise a client could POST a context that
        # overwrites status/block_reason and forge a blocked trace as "clean".
        event: dict = {}
        if context:
            event.update({k: v for k, v in context.items() if v is not None})
        event.update({
            "trace_id":     coerce_trace_uuid(trace_id) or trace_id,
            "type":         "llm",
            "status":       "blocked",
            "success":      False,
            "output":       result.block_reason,
            "block_reason": result.block_reason,
            "risk_level":   result.risk_level,
            "attack_types": result.attack_types,
        })
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


# ── Route ─────────────────────────────────────────────────────────────────────

@router.post("/secure/check", response_model=CheckResponse)
async def pre_call_check(
    payload: CheckRequest,
    api_key: Optional[str] = Depends(extract_api_key),
) -> CheckResponse:
    """Pre-call security guard with per-org guardrail policy.

    Order matters: hard blocks (deny-list, then high-risk attack patterns) are
    evaluated BEFORE the allow-list, so an attacker cannot neutralize a scan by
    embedding an allow-listed phrase next to a payload. The allow-list only
    short-circuits the softer worker/medium tier, never a deny or high-risk hit.
    """
    org_id, prefix = await _resolve_and_gate(api_key or payload.api_key)
    policy = await get_policy(org_id, slug=payload.guardrail or "default")

    # Cap the text actually scanned so a giant prompt can't drive unbounded regex
    # work. The prompt forwarded to the worker is likewise bounded.
    scan_prompt = payload.prompt[:_MAX_PROMPT_CHARS]

    # 1. Deny-list — hard block, evaluated first.
    deny_reason = _apply_deny_list(scan_prompt, policy)
    if deny_reason:
        response = CheckResponse(
            allow=False, block_reason=deny_reason, risk_level="high", attack_types=["custom_deny_list"],
        )
        if payload.trace_id:
            await _publish_blocked_trace(str(org_id), prefix, payload.trace_id, response, payload.context)
        _maybe_fire_webhook(policy, str(org_id), payload.trace_id, "high", ["custom_deny_list"])
        return response

    # 2. Fast pattern check — deterministic, catches obvious injection/jailbreak/
    #    skeleton-key including indirect attacks hidden in HTML markup. Runs
    #    before the allow-list so an allow phrase can't whitelist an injection.
    r = scanners.check(scan_prompt)
    pattern_attacks = _filter_by_categories(r.attack_types, policy)
    pattern_blocks = (
        r.risk_level == "high"
        or (r.risk_level == "medium" and policy.block_threshold == "medium")
    ) and bool(pattern_attacks or not policy.block_categories)

    if pattern_blocks:
        response = CheckResponse(
            allow        = False,
            block_reason = f"Blocked by fluiq.secure: {', '.join(pattern_attacks)}",
            risk_level   = r.risk_level,
            attack_types = pattern_attacks,
        )
        if payload.trace_id:
            await _publish_blocked_trace(str(org_id), prefix, payload.trace_id, response, payload.context)
        _maybe_fire_webhook(policy, str(org_id), payload.trace_id, r.risk_level, pattern_attacks)
        return response

    # 3. Allow-list short-circuit — only reachable once the hard blocks above have
    #    passed. Trusted content skips the deeper worker/medium-tier scan.
    if _apply_allow_list(scan_prompt, policy):
        return CheckResponse(allow=True, block_reason=None, risk_level="clean", attack_types=[])

    # 4. Priority path: full scan via evaluator worker (policy forwarded).
    #    Register the reply future BEFORE publishing so a fast worker reply is
    #    never lost to a registration race (which would force the degraded path).
    correlation_id = uuid4().hex
    try:
        reply = wait_for_reply(
            correlation_id,
            timeout=config.KAFKA_SECURITY_CHECK_TIMEOUT,
            expected_org=str(org_id),
        )
        await kafka_queue.add_job(
            {
                "operation":      "security_check_sync",
                "prompt":         scan_prompt,
                "correlation_id": correlation_id,
                "org_id":         str(org_id),
                "policy": {
                    "block_threshold":  policy.block_threshold,
                    "block_categories": policy.block_categories,
                    "pii_ignore":       policy.pii_ignore,
                },
            },
            topic=config.KAFKA_SECURITY_TOPIC,
        )
        result = await reply
        if result is not None:
            response = CheckResponse(**result)
            if not response.allow and payload.trace_id:
                await _publish_blocked_trace(str(org_id), prefix, payload.trace_id, response, payload.context)
            if not response.allow:
                _maybe_fire_webhook(policy, str(org_id), payload.trace_id, response.risk_level, response.attack_types)
            return response
    except Exception:
        logger.exception("[SECURE] Kafka full-scan failed, falling back to pattern check")

    # 5. Degraded fallback: the full worker scan (PII / secrets / semantic) did
    #    not run. Re-use the pattern result with policy applied. When
    #    SECURE_FAIL_CLOSED is set, block instead of allowing on degraded.
    logger.warning("[SECURE] Degraded pattern-only check for correlation_id=%s", correlation_id)
    attack_types = _filter_by_categories(r.attack_types, policy)
    should_block  = (
        r.risk_level == "high"
        or (r.risk_level == "medium" and policy.block_threshold == "medium")
    ) and bool(attack_types or not policy.block_categories)
    if config.SECURE_FAIL_CLOSED and not should_block:
        should_block = True
        attack_types = attack_types or ["scan_unavailable"]

    response = CheckResponse(
        allow        = not should_block,
        block_reason = f"Blocked by fluiq.secure: {', '.join(attack_types)}" if should_block else None,
        risk_level   = r.risk_level,
        attack_types = attack_types,
        degraded     = True,
    )
    if not response.allow and payload.trace_id:
        await _publish_blocked_trace(str(org_id), prefix, payload.trace_id, response, payload.context)
    if not response.allow:
        _maybe_fire_webhook(policy, str(org_id), payload.trace_id, response.risk_level, response.attack_types)
    return response
