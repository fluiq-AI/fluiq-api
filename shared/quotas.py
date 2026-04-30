"""Tier-based quotas for trace ingestion and automated evaluations.

The pricing page advertises four tiers (Free / Team / Growth / Enterprise)
with caps on lifetime traces and on LLM-as-judge evaluations. This module
holds the canonical mapping plus a tiny in-process TTL cache so the hot
``/ingest`` path can check usage without hammering ClickHouse on every
request.

Counts are pulled from ClickHouse (``fluiq.traces`` and ``fluiq.evaluations``)
filtered on ``organization_id``, which is the leading sort key for both
tables, so a ``count()`` is a fast index probe.
"""
from __future__ import annotations

import time
import uuid
from dataclasses import dataclass
from typing import Optional

from db_queues.clickhouse import clickhouse_client
from db_queues.postgresql.auth import get_org_tier

UNLIMITED = -1

# (trace_quota, eval_quota); -1 means unlimited.
TIER_QUOTAS: dict[str, tuple[int, int]] = {
    "Free":       (5_000_000,   1_000),
    "Team":       (UNLIMITED,  10_000),
    "Growth":     (UNLIMITED, 100_000),
    "Enterprise": (UNLIMITED, UNLIMITED),
}

DEFAULT_TIER = "Free"

# Refresh ClickHouse counts at most every 60 seconds per org. This is a soft
# cap — over-shooting by a minute's worth of traffic is acceptable, and the
# alternative (counting on every ingest) is unaffordable on the hot path.
_CACHE_TTL_SECONDS = 60.0


@dataclass
class _CachedCount:
    value: int
    expires_at: float


_trace_count_cache: dict[uuid.UUID, _CachedCount] = {}
_eval_count_cache: dict[uuid.UUID, _CachedCount] = {}
_tier_cache: dict[uuid.UUID, _CachedCount] = {}  # value held in .value as id
_tier_value_cache: dict[uuid.UUID, str] = {}


def _now() -> float:
    return time.monotonic()


async def _cached_trace_count(org_id: uuid.UUID) -> int:
    hit = _trace_count_cache.get(org_id)
    if hit is not None and hit.expires_at > _now():
        return hit.value
    value = await clickhouse_client.count_traces(org_id)
    _trace_count_cache[org_id] = _CachedCount(value, _now() + _CACHE_TTL_SECONDS)
    return value


async def _cached_eval_count(org_id: uuid.UUID) -> int:
    hit = _eval_count_cache.get(org_id)
    if hit is not None and hit.expires_at > _now():
        return hit.value
    value = await clickhouse_client.count_evaluations(org_id)
    _eval_count_cache[org_id] = _CachedCount(value, _now() + _CACHE_TTL_SECONDS)
    return value


async def _cached_tier(org_id: uuid.UUID) -> str:
    marker = _tier_cache.get(org_id)
    if marker is not None and marker.expires_at > _now():
        return _tier_value_cache.get(org_id, DEFAULT_TIER)
    tier = await get_org_tier(org_id) or DEFAULT_TIER
    _tier_value_cache[org_id] = tier
    _tier_cache[org_id] = _CachedCount(0, _now() + _CACHE_TTL_SECONDS)
    return tier


def _bump_cached(cache: dict[uuid.UUID, _CachedCount], org_id: uuid.UUID) -> None:
    hit = cache.get(org_id)
    if hit is not None and hit.expires_at > _now():
        hit.value += 1


def bump_trace_count(org_id: uuid.UUID) -> None:
    """Optimistically advance the cached trace count after a successful enqueue.

    Avoids letting bursts during a single TTL window slip past the quota
    just because the cached count is stale.
    """
    _bump_cached(_trace_count_cache, org_id)


def bump_eval_count(org_id: uuid.UUID) -> None:
    _bump_cached(_eval_count_cache, org_id)


@dataclass
class QuotaStatus:
    tier: str
    trace_count: int
    trace_quota: int
    eval_count: int
    eval_quota: int

    @property
    def trace_over(self) -> bool:
        return self.trace_quota != UNLIMITED and self.trace_count >= self.trace_quota

    @property
    def eval_over(self) -> bool:
        return self.eval_quota != UNLIMITED and self.eval_count >= self.eval_quota


async def get_quota_status(
    org_id: uuid.UUID,
    force_count: bool = False,
) -> QuotaStatus:
    """Resolve the org's tier and current usage.

    By default, counts are skipped for unlimited tiers since the hot
    ``/ingest`` path only needs them to check the cap. Pass
    ``force_count=True`` from read-only callers (e.g. the dashboard ``/quota``
    endpoint) that want real usage numbers regardless of the cap.
    """
    tier = await _cached_tier(org_id)
    trace_quota, eval_quota = TIER_QUOTAS.get(tier, TIER_QUOTAS[DEFAULT_TIER])

    trace_count = (
        await _cached_trace_count(org_id)
        if force_count or trace_quota != UNLIMITED
        else 0
    )
    eval_count = (
        await _cached_eval_count(org_id)
        if force_count or eval_quota != UNLIMITED
        else 0
    )
    return QuotaStatus(
        tier=tier,
        trace_count=trace_count,
        trace_quota=trace_quota,
        eval_count=eval_count,
        eval_quota=eval_quota,
    )


def invalidate(org_id: Optional[uuid.UUID] = None) -> None:
    """Drop cached entries (used by tests / admin tooling)."""
    if org_id is None:
        _trace_count_cache.clear()
        _eval_count_cache.clear()
        _tier_cache.clear()
        _tier_value_cache.clear()
        return
    _trace_count_cache.pop(org_id, None)
    _eval_count_cache.pop(org_id, None)
    _tier_cache.pop(org_id, None)
    _tier_value_cache.pop(org_id, None)
