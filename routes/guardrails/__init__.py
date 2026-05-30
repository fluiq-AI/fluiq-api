"""GET  /api/v1/guardrails        — read policy by slug
   PUT  /api/v1/guardrails        — save policy by slug (creates if new)
   GET  /api/v1/guardrails/list   — list all slugs for the org
   DELETE /api/v1/guardrails      — delete a non-default policy by slug
"""
from __future__ import annotations

import re
import uuid
from typing import List, Optional

from fastapi import APIRouter, Depends, HTTPException, Query, status
from pydantic import BaseModel, Field

from db_queues.postgresql.guardrails import (
    ALL_CATEGORIES,
    GuardrailPolicy,
    delete_policy,
    get_policy,
    list_slugs,
    upsert_policy,
)
from routes.auth.helper import get_current_session

guardrails_router = APIRouter()

_VALID_THRESHOLDS = {"low", "medium", "high"}
_SLUG_RE          = re.compile(r'^[a-z0-9][a-z0-9\-_]{0,62}$')


def _validate_slug(slug: str) -> None:
    if not _SLUG_RE.match(slug):
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="Slug must be lowercase alphanumeric with hyphens/underscores, 1–63 chars.",
        )


class GuardrailPolicyPayload(BaseModel):
    block_threshold:   str              = Field("high", pattern="^(medium|high)$")
    warn_threshold:    str              = Field("medium", pattern="^(low|medium|high)$")
    block_categories:  List[str]        = Field(default_factory=list)
    custom_deny_list:  List[str]        = Field(default_factory=list)
    custom_allow_list: List[str]        = Field(default_factory=list)
    alert_webhook:     Optional[str]    = None
    alert_on:          List[str]        = Field(default_factory=lambda: ["high"])
    scan_responses:    bool             = False


class GuardrailPolicyResponse(GuardrailPolicyPayload):
    org_id: str
    slug:   str


@guardrails_router.get("/guardrails/list")
async def list_policies(
    session: dict = Depends(get_current_session),
) -> list[str]:
    org_id = uuid.UUID(session["org_id"])
    return await list_slugs(org_id)


@guardrails_router.get("/guardrails", response_model=GuardrailPolicyResponse)
async def read_policy(
    slug:    str  = Query(default="default"),
    session: dict = Depends(get_current_session),
) -> GuardrailPolicyResponse:
    org_id = uuid.UUID(session["org_id"])
    policy = await get_policy(org_id, slug=slug)
    return GuardrailPolicyResponse(**policy.to_dict())


@guardrails_router.put("/guardrails", response_model=GuardrailPolicyResponse)
async def save_policy(
    payload: GuardrailPolicyPayload,
    slug:    str  = Query(default="default"),
    session: dict = Depends(get_current_session),
) -> GuardrailPolicyResponse:
    org_id = uuid.UUID(session["org_id"])
    _validate_slug(slug)

    bad = [c for c in payload.block_categories if c not in ALL_CATEGORIES]
    if bad:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=f"Unknown block_categories: {bad}. Valid: {ALL_CATEGORIES}",
        )
    bad_on = [r for r in payload.alert_on if r not in _VALID_THRESHOLDS]
    if bad_on:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=f"Invalid alert_on values: {bad_on}",
        )

    deny  = [p.strip() for p in payload.custom_deny_list  if p.strip()]
    allow = [p.strip() for p in payload.custom_allow_list if p.strip()]

    policy = GuardrailPolicy(
        org_id            = str(org_id),
        slug              = slug,
        block_threshold   = payload.block_threshold,
        warn_threshold    = payload.warn_threshold,
        block_categories  = payload.block_categories,
        custom_deny_list  = deny,
        custom_allow_list = allow,
        alert_webhook     = payload.alert_webhook or None,
        alert_on          = payload.alert_on,
        scan_responses    = payload.scan_responses,
    )
    saved = await upsert_policy(org_id, policy)
    return GuardrailPolicyResponse(**saved.to_dict())


@guardrails_router.delete("/guardrails", status_code=status.HTTP_204_NO_CONTENT)
async def remove_policy(
    slug:    str  = Query(...),
    session: dict = Depends(get_current_session),
) -> None:
    if slug == "default":
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="The default policy cannot be deleted.",
        )
    org_id = uuid.UUID(session["org_id"])
    await delete_policy(org_id, slug)
