import uuid

from fastapi import APIRouter, Depends, HTTPException, Query, status
from pydantic import BaseModel

from db_queues.clickhouse import clickhouse_client
from db_queues.postgresql.auth import get_organization
from routes.auth.helper import get_current_session


class CacheKindStats(BaseModel):
    kind: str
    hits: int
    misses: int
    calls: int
    hit_rate: float


class CacheStatsResponse(BaseModel):
    window_hours: int
    hits: int
    misses: int
    calls: int
    hit_rate: float
    per_kind: list[CacheKindStats]


optimize_router = APIRouter()


@optimize_router.get("/cache-stats", response_model=CacheStatsResponse)
async def get_cache_stats(
    session: dict = Depends(get_current_session),
    window_hours: int = Query(default=24, ge=1, le=720),
) -> CacheStatsResponse:
    """Return cache hit/miss totals over the last ``window_hours``.

    Counts are sourced from ``cache`` spans emitted by SDK caches running
    with ``trace=True``. When no spans have been ingested the response is
    a zeroed envelope so the dashboard can render an empty state without
    extra branching.
    """
    org_id = uuid.UUID(session["org_id"])
    organization = await get_organization(org_id)
    if organization is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Organization not found",
        )
    stats = await clickhouse_client.fetch_cache_stats(
        organization_id=org_id,
        window_hours=window_hours,
    )
    return CacheStatsResponse(
        window_hours=stats["window_hours"],
        hits=stats["hits"],
        misses=stats["misses"],
        calls=stats["calls"],
        hit_rate=stats["hit_rate"],
        per_kind=[CacheKindStats(**row) for row in stats["per_kind"]],
    )


__all__ = ["optimize_router", "CacheStatsResponse", "CacheKindStats"]
