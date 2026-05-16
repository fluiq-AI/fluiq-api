"""fluiq-api  —  POST /api/v1/secure  &  POST /api/v1/secure/check

POST /api/v1/secure       — full post-call scan (PII + all attack types)
POST /api/v1/secure/check — lightweight pre-call check (attack patterns only, fast)

Both require Team tier or above; return 402 for Free accounts.
"""
from __future__ import annotations

from typing import List, Optional

from fastapi import APIRouter, HTTPException, status
from pydantic import BaseModel

from db_queues.postgresql.auth import resolve_api_key, get_org_tier
from . import scanners

router = APIRouter()

_SECURE_TIERS = {"Team", "Growth", "Enterprise"}


# ── Auth helper ───────────────────────────────────────────────────────────────

async def _resolve_and_gate(api_key: str) -> None:
    if not api_key:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="API key required")
    resolved = await resolve_api_key(api_key)
    if resolved is None:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid API key")
    org_id, _prefix, _key_id = resolved
    tier = await get_org_tier(org_id) or "Free"
    if tier not in _SECURE_TIERS:
        raise HTTPException(
            status_code=status.HTTP_402_PAYMENT_REQUIRED,
            detail=(
                f"fluiq.secure() requires Team plan or above "
                f"(current plan: {tier}). Upgrade at app.getfluiq.com/billing."
            ),
        )


# ── POST /secure  (full post-call scan) ───────────────────────────────────────

class SecureRequest(BaseModel):
    api_key:      str
    prompt:       str       = ""
    response:     str       = ""
    tool_outputs: List[str] = []
    context_docs: List[str] = []


class SecureResponse(BaseModel):
    # PII
    prompt_redacted:       str
    response_redacted:     str
    pii_entities_prompt:   List[str]
    pii_entities_response: List[str]
    # Attacks
    injection_detected:    bool
    injection_patterns:    List[str]
    jailbreak_detected:    bool
    jailbreak_patterns:    List[str]
    skeleton_key_detected: bool
    skeleton_key_patterns: List[str]
    # Secrets
    secrets_detected:      bool
    secret_types:          List[str]
    # Indirect injection
    indirect_injection_detected: bool
    indirect_injection_sources:  List[str]
    # Semantic
    semantic_attack_score: float
    # Aggregate
    security_risk_level:   str
    security_risk_score:   float
    should_block:          bool


@router.post("/secure", response_model=SecureResponse)
async def secure_scan(payload: SecureRequest) -> SecureResponse:
    """Full post-call scan: PII, jailbreak, injection, skeleton key, secrets,
    indirect injection in tool outputs and context docs.

    Returns enriched security fields.  When ``should_block`` is ``True``
    the caller should substitute the redacted versions before persisting.
    """
    await _resolve_and_gate(payload.api_key)

    r = scanners.scan(
        prompt       = payload.prompt,
        response     = payload.response,
        tool_outputs = payload.tool_outputs,
        context_docs = payload.context_docs,
    )

    return SecureResponse(
        prompt_redacted             = r.prompt_redacted,
        response_redacted           = r.response_redacted,
        pii_entities_prompt         = r.pii_entities_prompt,
        pii_entities_response       = r.pii_entities_response,
        injection_detected          = r.injection_detected,
        injection_patterns          = r.injection_patterns,
        jailbreak_detected          = r.jailbreak_detected,
        jailbreak_patterns          = r.jailbreak_patterns,
        skeleton_key_detected       = r.skeleton_key_detected,
        skeleton_key_patterns       = r.skeleton_key_patterns,
        secrets_detected            = r.secrets_detected,
        secret_types                = r.secret_types,
        indirect_injection_detected = r.indirect_injection_detected,
        indirect_injection_sources  = r.indirect_injection_sources,
        semantic_attack_score       = r.semantic_attack_score,
        security_risk_level         = r.security_risk_level,
        security_risk_score         = r.security_risk_score,
        should_block                = r.should_block,
    )


# ── POST /secure/check  (lightweight pre-call guard) ─────────────────────────

class CheckRequest(BaseModel):
    api_key: str
    prompt:  str


class CheckResponse(BaseModel):
    allow:        bool
    block_reason: Optional[str]
    risk_level:   str
    attack_types: List[str]


@router.post("/secure/check", response_model=CheckResponse)
async def pre_call_check(payload: CheckRequest) -> CheckResponse:
    """Lightweight pre-call guard.  Runs attack-pattern + semantic checks on
    the *prompt only* (no response text required).  Designed to be fast
    enough to call on every LLM request when ``fluiq.secure(mode='block')``
    is active.

    Returns ``allow=False`` when the risk level is HIGH so the SDK can raise
    ``FluiqSecurityError`` before the LLM call is made.
    """
    await _resolve_and_gate(payload.api_key)
    r = scanners.check(payload.prompt)
    return CheckResponse(
        allow        = r.allow,
        block_reason = r.block_reason,
        risk_level   = r.risk_level,
        attack_types = r.attack_types,
    )
