"""In-memory registry of in-flight (running) trace events.

The Kafka tracer pipeline never persists ``status="running"`` events to
ClickHouse — they are pure live-progress signals fanned out via the SSE
broker (see :mod:`realtime.consumer`). That means a user who reloads the
dashboard mid-run sees nothing until the run completes and a real row
lands in ClickHouse.

This registry closes that gap. Every replica's
:class:`~realtime.consumer.TraceConsumer` mirrors started events into the
registry and evicts them when the matching completion / enrichment lands.
:func:`routes.trace.list_traces` then merges the registry's entries for
the caller's organization into the GET ``/api/v1/traces`` response so a
fresh page load surfaces in-flight runs.

Entries carry a monotonic timestamp; a stale entry (run that crashed
before completion) is swept from any reader that asks for entries older
than ``RUNNING_TTL_SECONDS``. No background task — sweeps happen lazily
on every list call. Replicas do not coordinate; each holds its own view
of running events for SSE connections that landed on it.
"""
import asyncio
import logging
import time
from typing import Any, Optional

logger = logging.getLogger(__name__)


# Stale runs are dropped after this many seconds. A run that the user
# kills (Ctrl-C) or that crashes mid-flight stays in the registry until
# this deadline, then quietly disappears from new page loads.
RUNNING_TTL_SECONDS = 600.0


class RunningTraceRegistry:
    def __init__(self) -> None:
        self._entries: dict[tuple[str, str], dict[str, Any]] = {}
        self._lock = asyncio.Lock()

    async def register(self, payload: dict) -> None:
        """Record a started trace.

        Idempotent on ``trace_id`` — a replayed start event overwrites
        the previous timestamp without creating a duplicate row.
        """
        org_id = payload.get("organization_id")
        trace_id = payload.get("trace_id")
        if not org_id or not trace_id:
            return
        entry = {
            "api_key_prefix": payload.get("api_key_prefix") or "",
            "trace_id": str(trace_id),
            "ingested_at_ms": payload.get("ingested_at_ms"),
            "event": payload.get("event") or {},
            "_registered_at": time.monotonic(),
        }
        async with self._lock:
            self._entries[(str(org_id), str(trace_id))] = entry

    async def complete(self, payload: dict) -> None:
        """Evict the running entry for ``trace_id`` if present.

        Called for both ``kind="persisted"`` and ``kind="enriched"``
        messages so a started row vanishes the moment its durable
        counterpart lands. Idempotent: missing keys are a no-op.
        """
        org_id = payload.get("organization_id")
        trace_id = payload.get("trace_id")
        if not org_id or not trace_id:
            return
        async with self._lock:
            self._entries.pop((str(org_id), str(trace_id)), None)

    async def list_for(
        self,
        organization_id: str,
        api_key_prefix: Optional[str] = None,
    ) -> list[dict[str, Any]]:
        """Return non-stale entries scoped to ``organization_id``.

        When ``api_key_prefix`` is provided, entries are filtered to that
        key (matching the same scoping used by the persisted-traces
        query). Stale entries (older than :data:`RUNNING_TTL_SECONDS`)
        are evicted in-place during the sweep.
        """
        if not organization_id:
            return []
        cutoff = time.monotonic() - RUNNING_TTL_SECONDS
        keep: list[dict[str, Any]] = []
        evicted = 0
        async with self._lock:
            stale_keys: list[tuple[str, str]] = []
            for key, entry in self._entries.items():
                org, _trace_id = key
                if org != organization_id:
                    continue
                if entry["_registered_at"] < cutoff:
                    stale_keys.append(key)
                    continue
                if (
                    api_key_prefix is not None
                    and entry.get("api_key_prefix") != api_key_prefix
                ):
                    continue
                keep.append(entry)
            for key in stale_keys:
                self._entries.pop(key, None)
                evicted += 1
        if evicted:
            logger.info("[RUNNING] Swept %d stale entries org=%s", evicted, organization_id)
        # Newest first so the frontend's prepend semantics match the
        # ordering it uses for live SSE deliveries.
        keep.sort(
            key=lambda e: e.get("ingested_at_ms") or 0,
            reverse=True,
        )
        return keep

    async def size(self) -> int:
        async with self._lock:
            return len(self._entries)


running_registry = RunningTraceRegistry()


__all__ = [
    "RunningTraceRegistry",
    "running_registry",
    "RUNNING_TTL_SECONDS",
]
