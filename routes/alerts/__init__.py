"""fluiq-api  —  /api/v1/alerts

GET  /alerts        — read this org's alert settings (JWT auth)
PUT  /alerts        — save alert settings (JWT auth, plan-gated per section)
POST /alerts/test   — send a test message to the Slack webhook (JWT auth)

Both require a paid plan — mirrors the pricing page, where Slack alerts
ship from Starter up.
"""
from __future__ import annotations

import uuid
from typing import List, Optional

from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel, Field

from db_queues.postgresql.alerts import (
    EVAL_METRICS,
    SECURITY_CATEGORIES,
    VALID_DIGESTS,
    VALID_RISK,
    AlertSettings,
    get_settings,
    upsert_settings,
)
from db_queues.postgresql.auth import get_org_tier
from routes.auth.helper import get_current_session
from shared import slack

alerts_router = APIRouter()

_EVAL_TIERS     = {"Starter", "Team", "Growth", "Enterprise"}
# Security scanning is metered, not tier-gated (see routes/secure), so its
# alerts follow the paid tiers rather than Growth alone.
_SECURITY_TIERS = {"Starter", "Team", "Growth", "Enterprise"}


# ── Schemas ──────────────────────────────────────────────────────────────────

class EvalAlertPayload(BaseModel):
    enabled:            bool        = False
    metrics:            List[str]   = Field(default_factory=list)
    score_below:        float       = Field(0.7, ge=0.0, le=1.0)
    failure_rate_above: float       = Field(10.0, ge=0.0, le=100.0)


class SecurityAlertPayload(BaseModel):
    enabled:      bool        = False
    alert_on:     List[str]   = Field(default_factory=lambda: ["high"])
    categories:   List[str]   = Field(default_factory=list)
    blocked_only: bool        = True


class AlertSettingsPayload(BaseModel):
    slack_webhook: Optional[str]         = None
    digest:        str                   = Field("realtime")
    eval:          EvalAlertPayload      = Field(default_factory=EvalAlertPayload)
    security:      SecurityAlertPayload  = Field(default_factory=SecurityAlertPayload)


class TestPayload(BaseModel):
    slack_webhook: Optional[str] = None


# ── Helpers ──────────────────────────────────────────────────────────────────

def _validate(payload: AlertSettingsPayload) -> None:
    if payload.digest not in VALID_DIGESTS:
        raise HTTPException(422, f"digest must be one of {sorted(VALID_DIGESTS)}")
    bad_metrics = [m for m in payload.eval.metrics if m not in EVAL_METRICS]
    if bad_metrics:
        raise HTTPException(422, f"Unknown eval metrics: {bad_metrics}")
    bad_risk = [r for r in payload.security.alert_on if r not in VALID_RISK]
    if bad_risk:
        raise HTTPException(422, f"Invalid alert_on values: {bad_risk}")
    bad_cats = [c for c in payload.security.categories if c not in SECURITY_CATEGORIES]
    if bad_cats:
        raise HTTPException(422, f"Unknown security categories: {bad_cats}")
    if payload.slack_webhook and not payload.slack_webhook.startswith("https://"):
        raise HTTPException(422, "slack_webhook must be an https:// URL")


# ── Routes ───────────────────────────────────────────────────────────────────

@alerts_router.get("/alerts")
async def read_alerts(session: dict = Depends(get_current_session)) -> dict:
    org_id = uuid.UUID(session["org_id"])
    settings = await get_settings(org_id)
    return settings.to_dict()


@alerts_router.put("/alerts")
async def save_alerts(
    payload: AlertSettingsPayload,
    session: dict = Depends(get_current_session),
) -> dict:
    org_id = uuid.UUID(session["org_id"])
    _validate(payload)

    tier = (await get_org_tier(org_id)) or "Free"
    if payload.eval.enabled and tier not in _EVAL_TIERS:
        raise HTTPException(
            status.HTTP_402_PAYMENT_REQUIRED,
            f"Eval alerts require a paid plan (current: {tier}).",
        )
    if payload.security.enabled and tier not in _SECURITY_TIERS:
        raise HTTPException(
            status.HTTP_402_PAYMENT_REQUIRED,
            f"Security alerts require a paid plan (current: {tier}).",
        )

    settings = AlertSettings(
        org_id                  = str(org_id),
        slack_webhook           = (payload.slack_webhook or None),
        digest                  = payload.digest,
        eval_enabled            = payload.eval.enabled,
        eval_metrics            = payload.eval.metrics,
        eval_score_below        = payload.eval.score_below,
        eval_failure_rate_above = payload.eval.failure_rate_above,
        security_enabled        = payload.security.enabled,
        security_alert_on       = payload.security.alert_on,
        security_categories     = payload.security.categories,
        security_blocked_only   = payload.security.blocked_only,
    )
    saved = await upsert_settings(org_id, settings)
    return saved.to_dict()


@alerts_router.post("/alerts/test")
async def test_alert(
    payload: TestPayload,
    session: dict = Depends(get_current_session),
) -> dict:
    org_id = uuid.UUID(session["org_id"])
    webhook = payload.slack_webhook
    if not webhook:
        webhook = (await get_settings(org_id)).slack_webhook
    if not webhook:
        raise HTTPException(422, "No Slack webhook configured.")
    if not webhook.startswith("https://"):
        raise HTTPException(422, "slack_webhook must be an https:// URL")

    blocks, text = slack.build_test_message()
    ok = await slack.post_webhook(webhook, blocks, text)
    if not ok:
        raise HTTPException(502, "Slack rejected the message. Check the webhook URL.")
    return {"ok": True}
