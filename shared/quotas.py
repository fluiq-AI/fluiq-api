"""Tier-based quotas for trace ingestion and automated evaluations.

The pricing page advertises five tiers (Free / Starter / Team / Growth /
Enterprise)
with per-month caps on traces and on LLM-as-judge evaluations. This module
holds the canonical mapping plus a tiny in-process TTL cache so the hot
``/ingest`` path can check usage without hammering ClickHouse on every
request.

Counts are pulled from ClickHouse (``fluiq.traces`` and ``fluiq.evaluations``)
filtered on ``organization_id`` and the current calendar month, so usage
resets at each month boundary in line with the advertised quotas.
"""
from __future__ import annotations

import time
import uuid
from dataclasses import dataclass
from typing import Optional

from db_queues.clickhouse import clickhouse_client
from db_queues.postgresql.auth import get_org_eval_bonus, get_org_tier

UNLIMITED = -1

# (trace_quota, eval_quota) per calendar month; -1 means unlimited.
#
# Observability (trace ingestion) is free and UNLIMITED on every tier,
# including Free — collecting traces is the whole point, and it's cheap
# relative to the compute-heavy pillars. We monetize evals / security, not
# trace volume. What differs by tier is *retention*, not the ingest cap
# (see TIER_RETENTION_DAYS): Free keeps a rolling window, paid keeps forever.
TIER_QUOTAS: dict[str, tuple[int, int]] = {
    "Free":       (UNLIMITED,     100),
    "Starter":    (UNLIMITED,   2_000),
    "Team":       (UNLIMITED,  10_000),
    "Growth":     (UNLIMITED,  50_000),
    "Enterprise": (UNLIMITED, UNLIMITED),
}

# Security scans are metered separately from evals because they cost something
# completely different to run: scanning is regex + spaCy NER with no LLM call
# at all, roughly $0.0002 a scan against $0.03+ for an agentic evaluation.
# Metering them like evals would price the cheapest thing we do as if it were
# the most expensive, and tax the coverage we actually want customers to have.
#
# Free gets a real allowance rather than zero: security is the pillar no eval
# competitor ships, and a pillar nobody can try is a pillar nobody buys.
TIER_SECURITY_QUOTAS: dict[str, int] = {
    "Free":         1_000,
    "Starter":     50_000,
    "Team":       500_000,
    "Growth":   2_000_000,
    "Enterprise":  UNLIMITED,
}


def security_quota_for_tier(tier: str) -> int:
    """Monthly security-scan allowance. Unknown tiers fall back to Free."""
    return TIER_SECURITY_QUOTAS.get(tier, TIER_SECURITY_QUOTAS["Free"])

DEFAULT_TIER = "Free"

# Raw-trace retention per tier, in days. Free orgs keep a rolling 14-day
# window; paid tiers never expire — the sentinel below is ~100 years, which
# ClickHouse's per-row TTL treats as "keep forever". The value is stamped onto
# each trace row at ingest so the TTL can roll off Free traces without ever
# touching paid data. Stays within UInt16 (max 65535) for the CH column.
FREE_RETENTION_DAYS = 14
UNLIMITED_RETENTION_DAYS = 36_500  # ~100 years ≈ never

TIER_RETENTION_DAYS: dict[str, int] = {
    "Free":       FREE_RETENTION_DAYS,
    "Starter":    UNLIMITED_RETENTION_DAYS,
    "Team":       UNLIMITED_RETENTION_DAYS,
    "Growth":     UNLIMITED_RETENTION_DAYS,
    "Enterprise": UNLIMITED_RETENTION_DAYS,
}


def retention_days_for_tier(tier: str) -> int:
    """Days to retain raw traces for an org on ``tier``.

    An unknown tier falls back to the Free window — failing toward *less*
    retention for an unrecognized (hence non-paying) tier, never toward
    silently keeping data forever.
    """
    return TIER_RETENTION_DAYS.get(tier, FREE_RETENTION_DAYS)

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
_security_count_cache: dict[uuid.UUID, _CachedCount] = {}
_tier_cache: dict[uuid.UUID, _CachedCount] = {}  # value held in .value as id
_tier_value_cache: dict[uuid.UUID, str] = {}
# Admin-granted eval allowance adjustment per org (may be negative). Stored as
# the int value directly in ``.value`` since it can be negative.
_eval_bonus_cache: dict[uuid.UUID, _CachedCount] = {}


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


async def _cached_security_count(org_id: uuid.UUID) -> int:
    hit = _security_count_cache.get(org_id)
    if hit is not None and hit.expires_at > _now():
        return hit.value
    value = await clickhouse_client.count_security_scans(org_id)
    _security_count_cache[org_id] = _CachedCount(value, _now() + _CACHE_TTL_SECONDS)
    return value


async def _cached_eval_bonus(org_id: uuid.UUID) -> int:
    hit = _eval_bonus_cache.get(org_id)
    if hit is not None and hit.expires_at > _now():
        return hit.value
    value = await get_org_eval_bonus(org_id)
    _eval_bonus_cache[org_id] = _CachedCount(value, _now() + _CACHE_TTL_SECONDS)
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


def bump_security_count(org_id: uuid.UUID) -> None:
    _bump_cached(_security_count_cache, org_id)


@dataclass
class QuotaStatus:
    tier: str
    trace_count: int
    trace_quota: int
    eval_count: int
    eval_quota: int
    retention_days: int = FREE_RETENTION_DAYS
    security_count: int = 0
    security_quota: int = 0

    @property
    def trace_over(self) -> bool:
        return self.trace_quota != UNLIMITED and self.trace_count >= self.trace_quota

    @property
    def eval_over(self) -> bool:
        return self.eval_quota != UNLIMITED and self.eval_count >= self.eval_quota

    @property
    def security_over(self) -> bool:
        return (
            self.security_quota != UNLIMITED
            and self.security_count >= self.security_quota
        )


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

    # Admins can grant (or deduct) extra evaluations on top of the tier cap.
    # Unlimited tiers stay unlimited; bounded tiers floor at zero so a
    # deduction can zero out the allowance but never go negative.
    if eval_quota != UNLIMITED:
        bonus = await _cached_eval_bonus(org_id)
        eval_quota = max(0, eval_quota + bonus)

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
    security_quota = security_quota_for_tier(tier)
    security_count = (
        await _cached_security_count(org_id)
        if force_count or security_quota != UNLIMITED
        else 0
    )
    return QuotaStatus(
        tier=tier,
        trace_count=trace_count,
        trace_quota=trace_quota,
        eval_count=eval_count,
        eval_quota=eval_quota,
        retention_days=retention_days_for_tier(tier),
        security_count=security_count,
        security_quota=security_quota,
    )


def invalidate(org_id: Optional[uuid.UUID] = None) -> None:
    """Drop cached entries (used by tests / admin tooling)."""
    if org_id is None:
        _trace_count_cache.clear()
        _eval_count_cache.clear()
        _security_count_cache.clear()
        _tier_cache.clear()
        _tier_value_cache.clear()
        _eval_bonus_cache.clear()
        return
    _trace_count_cache.pop(org_id, None)
    _eval_count_cache.pop(org_id, None)
    _security_count_cache.pop(org_id, None)
    _tier_cache.pop(org_id, None)
    _tier_value_cache.pop(org_id, None)
    _eval_bonus_cache.pop(org_id, None)
