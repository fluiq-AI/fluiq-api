import uuid

from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel

from db_queues.postgresql.auth import get_organization
from routes.auth.helper import get_current_session
from shared.quotas import UNLIMITED, get_quota_status


class QuotaCounter(BaseModel):
    used: int
    # ``None`` signals an unlimited tier so the frontend can render "Unlimited"
    # instead of trying to draw a progress bar against a sentinel value.
    limit: int | None


class QuotaResponse(BaseModel):
    tier: str
    traces: QuotaCounter
    evaluations: QuotaCounter


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
    )


__all__ = ["quota_router", "QuotaResponse", "QuotaCounter"]
