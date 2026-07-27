import uuid
from datetime import datetime
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel

from db_queues.clickhouse import clickhouse_client
from db_queues.postgresql.auth import get_organization, get_trial_ends_at
from routes.auth.helper import get_current_session
from shared.quotas import UNLIMITED, UNLIMITED_RETENTION_DAYS, get_quota_status


class QuotaCounter(BaseModel):
    used: int
    # ``None`` signals an unlimited tier so the frontend can render "Unlimited"
    # instead of trying to draw a progress bar against a sentinel value.
    limit: int | None


class QuotaResponse(BaseModel):
    tier: str
    traces: QuotaCounter
    evaluations: QuotaCounter
    # Metered separately from evals: scanning is regex + NER with no LLM call,
    # so it is orders of magnitude cheaper and gets its own allowance.
    security_scans: QuotaCounter
    # Raw-trace retention window in days for this tier. Paid tiers use a
    # ~100-year sentinel; ``None`` tells the frontend to render "Unlimited".
    retention_days: int | None
    # Expiry of an active self-serve trial, or ``None`` when the tier isn't a
    # trial. Lets the dashboard show "Trial · N days left".
    trial_ends_at: Optional[datetime] = None


class JudgeUsageBreakdown(BaseModel):
    evaluator: str
    input_tokens: int
    output_tokens: int
    judge_calls: int
    eval_runs: int


class JudgeUsageResponse(BaseModel):
    """Judge-token spend — the measurable part of what an evaluation costs.

    Tokens, not money: the price of a token is a pricing decision that will
    change, while the count is ground truth. Callers apply their own rate.
    """
    window_days: int
    input_tokens: int
    output_tokens: int
    judge_calls: int
    #: Distinct eval messages, i.e. the billable unit. Not metric rows — a jury
    #: writes one row and makes several calls.
    eval_runs: int
    by_evaluator: list[JudgeUsageBreakdown]


quota_router = APIRouter()


@quota_router.get("/quota", response_model=QuotaResponse)
async def get_quota(
    session: dict = Depends(get_current_session),
) -> QuotaResponse:
    """Return the caller org's tier and current usage against its quotas.

    Counts come from the same TTL-cached helpers used by ``/ingest`` so this
    endpoint is cheap to poll from the dashboard.
    """
    org_id = uuid.UUID(session["org_id"])
    organization = await get_organization(org_id)
    if organization is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Organization not found",
        )

    status_ = await get_quota_status(org_id, force_count=True)
    trial_ends_at = await get_trial_ends_at(org_id)
    return QuotaResponse(
        tier=status_.tier,
        traces=QuotaCounter(
            used=status_.trace_count,
            limit=None if status_.trace_quota == UNLIMITED else status_.trace_quota,
        ),
        evaluations=QuotaCounter(
            used=status_.eval_count,
            limit=None if status_.eval_quota == UNLIMITED else status_.eval_quota,
        ),
        security_scans=QuotaCounter(
            used=status_.security_count,
            limit=None if status_.security_quota == UNLIMITED else status_.security_quota,
        ),
        retention_days=(
            None
            if status_.retention_days >= UNLIMITED_RETENTION_DAYS
            else status_.retention_days
        ),
        trial_ends_at=trial_ends_at,
    )


@quota_router.get("/quota/judge-usage", response_model=JudgeUsageResponse)
async def get_judge_usage(
    days: int = 30,
    session: dict = Depends(get_current_session),
) -> JudgeUsageResponse:
    """Judge tokens this org has spent, in total and by evaluator.

    Aggregated in ClickHouse rather than here: the ``judge_*`` columns carry a
    whole message's totals on its first row, so only SUM over the window gives
    the right answer.
    """
    org_id = uuid.UUID(session["org_id"])
    if days < 1 or days > 365:
        raise HTTPException(422, "days must be between 1 and 365")

    usage = await clickhouse_client.fetch_judge_usage(org_id, days=days)
    return JudgeUsageResponse(
        window_days=usage["window_days"],
        input_tokens=usage["input_tokens"],
        output_tokens=usage["output_tokens"],
        judge_calls=usage["judge_calls"],
        eval_runs=usage["eval_runs"],
        by_evaluator=[JudgeUsageBreakdown(**row) for row in usage["by_evaluator"]],
    )


__all__ = [
    "quota_router",
    "QuotaResponse",
    "QuotaCounter",
    "JudgeUsageResponse",
    "JudgeUsageBreakdown",
]
