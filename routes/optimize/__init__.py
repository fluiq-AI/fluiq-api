"""fluiq-api  —  /api/v1/optimize/*

GET  /cache-stats   — dashboard: cache hit/miss totals (JWT auth)
GET  /profile       — SDK: optimization profile (API key auth, paid)
GET  /cache/{key}   — SDK: proxy Redis GET (API key auth)
POST /cache         — SDK: proxy Redis SET (API key auth)
GET  /evals         — CI: recent evaluation scores for eval gate (API key auth)
"""
import json
import uuid
from typing import Any, Optional

import config
from fastapi import APIRouter, Depends, HTTPException, Query, status
from pydantic import BaseModel

from db_queues.clickhouse import clickhouse_client
from db_queues.postgresql.auth import get_organization, get_org_tier, resolve_api_key
from routes.auth.helper import extract_api_key, get_current_session
from shared.cache import cached_json, dash_key

_OPTIMIZE_TIERS = {"Team", "Growth", "Enterprise"}


# ── Shared API-key auth helper ────────────────────────────────────────────────

async def _resolve_api_key_and_org(api_key: str) -> tuple[uuid.UUID, str]:
    """Resolve an SDK API key to (org_id, tier).  Raises 401 / 402 on failure."""
    if not api_key:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="API key required")
    resolved = await resolve_api_key(api_key)
    if resolved is None:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid API key")
    org_id = resolved[0]
    return org_id, (await get_org_tier(org_id)) or "Free"


optimize_router = APIRouter()


# ── GET /cache-stats  (dashboard, JWT auth) ───────────────────────────────────

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
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Organization not found")
    stats = await cached_json(
        dash_key(org_id, "cache_stats", window_hours=window_hours),
        30,
        lambda: clickhouse_client.fetch_cache_stats(
            organization_id=org_id,
            window_hours=window_hours,
        ),
    )
    return CacheStatsResponse(
        window_hours=stats["window_hours"],
        hits=stats["hits"],
        misses=stats["misses"],
        calls=stats["calls"],
        hit_rate=stats["hit_rate"],
        per_kind=[CacheKindStats(**row) for row in stats["per_kind"]],
    )


@optimize_router.get("/insights")
async def get_optimization_insights(
    session: dict = Depends(get_current_session),
    window_hours: int = Query(default=168, ge=1, le=720),
) -> dict:
    """Developer optimization insights (JWT auth) — powers the Insights page.

    Read-only analysis over the org's traces + costs in the window: most-repeated
    prompts (cache candidates + projected savings), a cacheable-spend headline,
    the most expensive models and agents, the slowest models (p95), and error
    hotspots. Cached briefly so repeated dashboard loads don't re-scan.
    """
    org_id = uuid.UUID(session["org_id"])
    return await cached_json(
        dash_key(org_id, "optimization_insights", window_hours=window_hours),
        60,
        lambda: clickhouse_client.fetch_optimization_insights(
            organization_id=org_id,
            window_hours=window_hours,
        ),
    )


# ── GET /profile  (SDK, API key auth, paid tier) ──────────────────────────────

class ProfileResponse(BaseModel):
    redis_url: Optional[str] = None
    key_prefix: str
    models: list[str]
    ttl_seconds: int
    estimated_hit_rate: float
    window_hours: int


@optimize_router.get("/profile", response_model=ProfileResponse)
async def get_optimization_profile(
    api_key: Optional[str] = Depends(extract_api_key),
    window_hours: int = Query(default=168, ge=1, le=720),
    min_calls: int = Query(default=10, ge=1),
) -> ProfileResponse:
    """Return the Redis cache profile for the calling org.

    Called by the SDK on the first LLM call after ``fluiq.optimize()`` is
    invoked.  Analyses the org's historical traces to identify which models
    are called most frequently and returns the connection details for the
    org's dedicated Redis instance.

    Requires Team plan or above.  Free accounts receive 402.
    """
    org_id, tier = await _resolve_api_key_and_org(api_key)
    if tier not in _OPTIMIZE_TIERS:
        raise HTTPException(
            status_code=status.HTTP_402_PAYMENT_REQUIRED,
            detail=(
                f"fluiq.optimize() requires Team plan or above "
                f"(current plan: {tier}). Upgrade at getfluiq.com/dashboard."
            ),
        )
    if not config.REDIS_SDK_URL:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Redis is not configured on this deployment.",
        )

    profile = await cached_json(
        dash_key(org_id, "optimization_profile", window_hours=window_hours, min_calls=min_calls),
        60,
        lambda: clickhouse_client.fetch_optimization_profile(
            organization_id=org_id,
            window_hours=window_hours,
            min_calls=min_calls,
        ),
    )

    # Per-org key namespace: first 8 hex chars of org_id for brevity
    key_prefix = f"fluiq:{org_id.hex[:8]}:"

    return ProfileResponse(
        redis_url=config.REDIS_SDK_URL,
        key_prefix=key_prefix,
        models=profile["models"],
        ttl_seconds=config.REDIS_DEFAULT_TTL_SECONDS,
        estimated_hit_rate=profile["estimated_hit_rate"],
        window_hours=profile["window_hours"],
    )


# ── Lazy async Redis client ───────────────────────────────────────────────────

_redis_client: Any = None


def _get_redis() -> Any:
    global _redis_client
    if _redis_client is None:
        import redis.asyncio as aioredis
        _redis_client = aioredis.from_url(
            config.REDIS_URL,
            decode_responses=True,
        )
    return _redis_client


def _org_cache_key(org_id: uuid.UUID, key: str) -> str:
    return f"fluiq:{org_id.hex[:8]}:{key}"


# ── GET /cache/{key}  (SDK proxy, API key auth) ───────────────────────────────

@optimize_router.get("/cache/{key}")
async def get_cache_entry(
    key: str,
    api_key: Optional[str] = Depends(extract_api_key),
) -> dict:
    """Proxy a Redis GET for the SDK.  Namespaced by org; no tier check needed
    (only reachable after a successful /profile fetch which already tier-gates)."""
    org_id, _ = await _resolve_api_key_and_org(api_key)
    if not config.REDIS_URL:
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail="Cache unavailable")
    try:
        r = _get_redis()
        raw = await r.get(_org_cache_key(org_id, key))
        if raw is None:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="miss")
        return {"value": json.loads(raw)}
    except HTTPException:
        raise
    except Exception:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="miss")


# ── POST /cache  (SDK proxy, API key auth) ────────────────────────────────────

class CacheSetRequest(BaseModel):
    key: str
    value: Any
    ttl: Optional[int] = None


@optimize_router.post("/cache", status_code=204)
async def set_cache_entry(
    body: CacheSetRequest,
    api_key: Optional[str] = Depends(extract_api_key),
) -> None:
    """Proxy a Redis SET for the SDK.  Namespaced by org; fire-and-forget from
    the SDK side — the response is not awaited by the caller."""
    org_id, _ = await _resolve_api_key_and_org(api_key)
    if not config.REDIS_URL:
        return
    try:
        r = _get_redis()
        full_key = _org_cache_key(org_id, body.key)
        serialized = json.dumps(body.value)
        effective_ttl = body.ttl if body.ttl is not None else config.REDIS_DEFAULT_TTL_SECONDS
        if effective_ttl:
            await r.setex(full_key, effective_ttl, serialized)
        else:
            await r.set(full_key, serialized)
    except Exception:
        pass


# ── GET /evals  (CI eval gate, API key auth) ──────────────────────────────────

class EvalEntry(BaseModel):
    trace_id: Optional[str]
    metric: str
    score: Optional[float]
    evaluator: str
    judge_model: str


class EvalsResponse(BaseModel):
    window_minutes: int
    total: int
    passed: int
    failed: int
    avg_score: Optional[float]
    entries: list[EvalEntry]


@optimize_router.get("/evals", response_model=EvalsResponse)
async def get_recent_evals(
    api_key: Optional[str] = Depends(extract_api_key),
    window_minutes: int = Query(default=30, ge=1, le=1440),
    threshold: float = Query(default=0.7, ge=0.0, le=1.0),
    limit: int = Query(default=200, ge=1, le=1000),
) -> EvalsResponse:
    """Return evaluation scores for the last ``window_minutes`` of traces.

    Designed for CI eval gates.  The caller passes ``threshold`` and
    inspects ``failed`` > 0 to decide whether to block the PR.

    Auth: SDK API key in ``x-api-key`` header (no login required — safe for CI).
    No tier gating — all accounts with at least one evaluation can use this.
    """
    org_id, _ = await _resolve_api_key_and_org(api_key)
    rows = await clickhouse_client.fetch_recent_evals(
        organization_id=org_id,
        window_minutes=window_minutes,
        limit=limit,
    )
    entries = [EvalEntry(**r) for r in rows]
    scores = [e.score for e in entries if e.score is not None]
    passed = sum(1 for s in scores if s >= threshold)
    failed = sum(1 for s in scores if s < threshold)
    avg_score = (sum(scores) / len(scores)) if scores else None
    return EvalsResponse(
        window_minutes=window_minutes,
        total=len(entries),
        passed=passed,
        failed=failed,
        avg_score=round(avg_score, 4) if avg_score is not None else None,
        entries=entries,
    )


# ── GET /prompt-cache-stats  (dashboard, JWT auth) ────────────────────────────

class PromptCacheStatsResponse(BaseModel):
    window_hours: int
    anthropic_cache_read_tokens: int
    anthropic_cache_creation_tokens: int
    provider_cached_tokens: int   # OpenAI + Gemini cached tokens
    total_cached_tokens: int
    calls: int
    calls_with_hit: int


@optimize_router.get("/prompt-cache-stats", response_model=PromptCacheStatsResponse)
async def get_prompt_cache_stats(
    session: dict = Depends(get_current_session),
    window_hours: int = Query(default=24, ge=1, le=720),
) -> PromptCacheStatsResponse:
    """Return provider-level prompt cache token counts over the last ``window_hours``.

    Aggregates Anthropic cache_read / cache_creation tokens and OpenAI
    cached_tokens emitted by SDK-instrumented LLM calls.
    """
    org_id = uuid.UUID(session["org_id"])
    organization = await get_organization(org_id)
    if organization is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Organization not found")
    stats = await cached_json(
        dash_key(org_id, "prompt_cache_stats", window_hours=window_hours),
        30,
        lambda: clickhouse_client.fetch_prompt_cache_stats(
            organization_id=org_id,
            window_hours=window_hours,
        ),
    )
    return PromptCacheStatsResponse(**stats)


__all__ = [
    "optimize_router",
    "CacheStatsResponse",
    "CacheKindStats",
    "ProfileResponse",
    "CacheSetRequest",
    "EvalsResponse",
    "EvalEntry",
    "PromptCacheStatsResponse",
]