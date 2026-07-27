"""Trace-identifier normalization.

ClickHouse stores ``trace_id`` / ``root_trace_id`` as UUID columns, so a
non-UUID identifier from a raw-API caller (the SDK always sends UUIDs) makes the
tracer's insert throw ``ValueError: invalid literal for int() with base 16`` and
fail the whole batch — the trace is lost and the consumer errors.

``coerce_trace_uuid`` maps any non-UUID string to a *deterministic* UUIDv5 so:
  • the tracer never receives a value it cannot store (no crash, no data loss);
  • the same input string always maps to the same UUID, so a caller that reuses
    a custom id as ``trace_id`` here and ``parent_id`` on a child event keeps the
    two linked after normalization.
Callers should surface the returned id (e.g. in the ingest response) so the
client can correlate against what was actually stored.
"""
from __future__ import annotations

import uuid
from typing import Optional

# Stable namespace so the mapping is reproducible across processes/restarts.
_TRACE_NS = uuid.uuid5(uuid.NAMESPACE_URL, "https://getfluiq.com/trace-id")


def coerce_trace_uuid(value: Optional[str]) -> Optional[str]:
    """Return ``value`` unchanged if it is already a valid UUID; map any other
    non-empty string to a deterministic UUIDv5; pass ``None``/empty through."""
    if value is None:
        return None
    s = str(value).strip()
    if not s:
        return None
    try:
        return str(uuid.UUID(s))
    except (ValueError, AttributeError, TypeError):
        return str(uuid.uuid5(_TRACE_NS, s))
