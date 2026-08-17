"""fluiq-api — Monitor: operational metrics over time.

  GET /api/v1/monitor        time series + per-model breakdown (session)

The Overview page answers "what am I paying and how much quota is left" — a
billing question. This answers the operational one: is the application slower,
dearer, or worse than it was yesterday, and which model changed?

Everything here is aggregated in ClickHouse. The endpoint returns at most a few
hundred rows regardless of traffic volume, because the alternative — pulling
traces and summing them client-side — is the pattern that made the Security tab
take 40 seconds.
"""
from __future__ import annotations

import uuid
from typing import Any, Dict, Optional

from fastapi import APIRouter, Depends, Query

from db_queues.clickhouse import clickhouse_client
from routes.auth.helper import get_current_session

monitor_router = APIRouter()

#: Window → bucket size. A fixed bucket would give 5-minute granularity over a
#: week (2,016 points, unreadable and slow) or hourly over an hour (one point).
#: Each pairing lands around 50–170 points, which is what a chart can show.
_BUCKETS = {
    1:   5,      # last hour      → 5-minute buckets
    6:   15,
    24:  30,
    72:  60,
    168: 240,    # last week      → 4-hour buckets
    720: 1440,   # last 30 days   → daily
}


def _bucket_for(hours: int) -> int:
    for window in sorted(_BUCKETS):
        if hours <= window:
            return _BUCKETS[window]
    return 1440


@monitor_router.get("/monitor")
async def get_monitor(
    hours: int = Query(24, ge=1, le=720),
    model: Optional[str] = Query(default=None),
    session: dict = Depends(get_current_session),
) -> Dict[str, Any]:
    """Operational time series for the window, plus a per-model breakdown."""
    org_id = uuid.UUID(session["org_id"])
    bucket = _bucket_for(hours)

    series = await clickhouse_client.fetch_monitor_series(
        org_id, hours=hours, bucket_minutes=bucket, model=model or None,
    )
    breakdown = await clickhouse_client.fetch_monitor_breakdown(org_id, hours=hours)

    return {
        "window_hours":   hours,
        "bucket_minutes": bucket,
        "model":          model or None,
        **series,
        "models":  breakdown,
        "totals":  _totals(series),
    }


def _totals(series: Dict[str, Any]) -> Dict[str, Any]:
    """Headline numbers for the window.

    Latency is the max of the bucket p95s rather than a true window-wide p95:
    percentiles do not average, and quoting the mean of them would understate
    the worst period — which is the one worth knowing about.
    """
    traffic = series.get("traffic") or []
    spend = series.get("spend") or []
    scores = series.get("scores") or []

    p95s = [b["p95"] for b in traffic if b.get("p95") is not None]
    judged = sum(b.get("judged") or 0 for b in scores)
    weighted = sum(
        (b.get("judge_score") or 0) * (b.get("judged") or 0) for b in scores
    )
    feedback_n = sum(b.get("feedback_count") or 0 for b in scores)
    feedback_sum = sum(
        (b.get("feedback_score") or 0) * (b.get("feedback_count") or 0) for b in scores
    )

    return {
        "spans":       sum(b.get("spans") or 0 for b in traffic),
        "runs":        sum(b.get("runs") or 0 for b in traffic),
        "errors":      sum(b.get("errors") or 0 for b in traffic),
        "cost":        sum(b.get("cost") or 0 for b in spend),
        "tokens":      sum(b.get("tokens") or 0 for b in spend),
        "worst_p95":   max(p95s) if p95s else None,
        # Weighted by how many were judged in each bucket, so a quiet hour with
        # one bad score doesn't drag the headline down as far as a busy one.
        "judge_score": (weighted / judged) if judged else None,
        "judged":      judged,
        "feedback_score": (feedback_sum / feedback_n) if feedback_n else None,
        "feedback_count": feedback_n,
    }


__all__ = ["monitor_router"]
