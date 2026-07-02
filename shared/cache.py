"""Fail-open Redis cache for hot, read-mostly dashboard aggregates.

The dashboard re-issues the same heavy ClickHouse aggregates constantly (the
profiling showed ~720k queries/week against tiny tables). These results tolerate
a few seconds of staleness, so a short-TTL cache collapses that volume.

Design: **fail-open** — any Redis hiccup (down, timeout, serialization) silently
falls back to computing the value uncached. Caching must never break a page.
"""
import hashlib
import json
import logging
from typing import Any, Awaitable, Callable

import config

logger = logging.getLogger(__name__)

_redis: Any = None


def _get_redis() -> Any:
    global _redis
    if _redis is None and config.REDIS_URL:
        import redis.asyncio as aioredis
        _redis = aioredis.from_url(config.REDIS_URL, decode_responses=True)
    return _redis


def dash_key(org_id: Any, name: str, **params: Any) -> str:
    """Stable per-org cache key for a dashboard aggregate + its parameters."""
    org8 = getattr(org_id, "hex", str(org_id))[:8]
    if params:
        blob = json.dumps(params, sort_keys=True, default=str)
        suffix = hashlib.sha1(blob.encode()).hexdigest()[:12]
        return f"fluiq:dash:{org8}:{name}:{suffix}"
    return f"fluiq:dash:{org8}:{name}"


async def cached_json(key: str, ttl: int, producer: Callable[[], Awaitable[Any]]) -> Any:
    """Return cached JSON for ``key``; else run ``producer()``, cache, return.

    Fail-open on every Redis path. Datetimes are serialized via ``default=str``
    (ISO strings); Pydantic response models coerce them back on the read side.
    """
    r = _get_redis()
    if r is not None:
        try:
            raw = await r.get(key)
            if raw is not None:
                return json.loads(raw)
        except Exception:
            logger.debug("[CACHE] get failed for %s", key, exc_info=True)
            r = None
    value = await producer()
    if r is not None:
        try:
            await r.setex(key, ttl, json.dumps(value, default=str))
        except Exception:
            logger.debug("[CACHE] set failed for %s", key, exc_info=True)
    return value
