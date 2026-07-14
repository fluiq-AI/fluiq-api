import uuid

from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel

from datetime import datetime
from typing import Optional

from db_queues.postgresql.auth import TRIAL_DAYS, TRIALABLE_TIERS, start_trial
from routes.auth.helper import get_current_session
from shared import quotas


class StartTrialRequest(BaseModel):
    plan: str


class StartTrialResponse(BaseModel):
    ok: bool
    tier: str
    trial_ends_at: Optional[datetime]
    trial_days: int


billing_router = APIRouter()


@billing_router.post("/billing/trial", response_model=StartTrialResponse)
async def begin_trial(
    body: StartTrialRequest,
    session: dict = Depends(get_current_session),
) -> StartTrialResponse:
    """Start a self-serve 5-day trial of a paid plan (Team / Growth), no card.

    The org owner is flipped to the requested tier with a ``trial_ends_at`` set
    5 days out; ``get_org_tier`` reverts it to Free on expiry. Eligibility is
    enforced in ``start_trial`` (Free-only, one trial per account); a rejection
    comes back as a 400 with a user-facing reason.
    """
    if body.plan not in TRIALABLE_TIERS:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Trials are only available for: {', '.join(sorted(TRIALABLE_TIERS))}.",
        )

    org_id = uuid.UUID(session["org_id"])
    result = await start_trial(org_id, body.plan)
    if not result.get("ok"):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=result.get("error", "Could not start trial."),
        )

    # Drop the org's cached tier so the new plan's features unlock immediately
    # instead of waiting out the quota cache's TTL window.
    quotas.invalidate(org_id)

    return StartTrialResponse(
        ok=True,
        tier=result["tier"],
        trial_ends_at=result.get("trial_ends_at"),
        trial_days=TRIAL_DAYS,
    )


__all__ = ["billing_router", "StartTrialRequest", "StartTrialResponse"]
