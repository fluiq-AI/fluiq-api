"""PostgreSQL helpers for online scoring rules.

A rule says "score this share of this kind of live traffic with these scorers".
It is read on **every trace ingest**, which is the hottest path in the API, so
reads go through a short-lived in-process cache: a rule change taking a few
seconds to apply is invisible to a user, whereas a Postgres round trip per trace
would not be.

The cache is per-process and per-org. Workers are stateless and horizontally
scaled, so a write invalidates only the process that served it; the TTL is what
actually bounds staleness everywhere else. That is the right trade here — the
alternative is a pub/sub invalidation channel for a feature whose worst failure
is "the new rule starts applying eight seconds later".
"""
from __future__ import annotations

import json
import time
import uuid
from typing import Any, Dict, List, Optional

from . import postgres_client

#: How long a fetched rule set is trusted. Short enough that toggling a rule
#: feels immediate, long enough that ingest never becomes a database read.
CACHE_TTL_SECONDS = 8.0

# org_id -> (expires_at, rules)
_cache: Dict[str, tuple[float, List[Dict[str, Any]]]] = {}


def _row_to_rule(row: Any) -> Dict[str, Any]:
    def _json(value: Any, fallback: Any) -> Any:
        if value is None:
            return fallback
        if isinstance(value, str):
            try:
                return json.loads(value)
            except ValueError:
                return fallback
        return value

    return {
        "rule_id":       str(row["rule_id"]),
        "project_id":    str(row["project_id"]) if row.get("project_id") else None,
        "name":          row["name"],
        "description":   row.get("description"),
        "enabled":       bool(row["enabled"]),
        "metrics":       _json(row.get("metrics"), []),
        "custom_judges": _json(row.get("custom_judges"), {}),
        "sample_rate":   float(row["sample_rate"]),
        "span_scope":    row.get("span_scope") or "root",
        "integrations":  _json(row.get("integrations"), []),
        "models":        _json(row.get("models"), []),
        "judge":         row.get("judge"),
        "created_at":    row["created_at"].isoformat() if row.get("created_at") else None,
        "updated_at":    row["updated_at"].isoformat() if row.get("updated_at") else None,
    }


def invalidate(org_id: uuid.UUID) -> None:
    """Drop this process's cached rules for an org, after a write."""
    _cache.pop(str(org_id), None)


async def list_rules(org_id: uuid.UUID) -> List[Dict[str, Any]]:
    """Every rule for an org, enabled or not. For the settings UI, not ingest."""
    async with postgres_client.acquire() as conn:
        rows = await conn.fetch(
            "SELECT * FROM online_scoring_rules WHERE org_id = $1 ORDER BY created_at",
            org_id,
        )
        return [_row_to_rule(r) for r in rows]


async def active_rules(org_id: uuid.UUID) -> List[Dict[str, Any]]:
    """Enabled rules for an org, cached. Called once per ingested trace.

    Never raises: a database blip must not stop traces being accepted. An org
    with unreadable rules is simply not scored online for a few seconds, which
    is the same outcome as having no rules.
    """
    key = str(org_id)
    hit = _cache.get(key)
    now = time.monotonic()
    if hit is not None and hit[0] > now:
        return hit[1]

    try:
        async with postgres_client.acquire() as conn:
            rows = await conn.fetch(
                "SELECT * FROM online_scoring_rules WHERE org_id = $1 AND enabled",
                org_id,
            )
        rules = [_row_to_rule(r) for r in rows]
    except Exception:  # noqa: BLE001 — ingest must not fail over a rules read
        # Cache the empty result briefly too, so a database outage doesn't turn
        # into a query storm from every ingest.
        rules = []

    _cache[key] = (now + CACHE_TTL_SECONDS, rules)
    return rules


async def create_rule(org_id: uuid.UUID, **fields: Any) -> Dict[str, Any]:
    async with postgres_client.acquire() as conn:
        row = await conn.fetchrow(
            """
            INSERT INTO online_scoring_rules
                (org_id, project_id, name, description, enabled, metrics,
                 custom_judges, sample_rate, span_scope, integrations, models, judge)
            VALUES ($1, $2, $3, $4, $5, $6::jsonb, $7::jsonb, $8, $9, $10::jsonb, $11::jsonb, $12)
            RETURNING *
            """,
            org_id,
            fields.get("project_id"),
            fields["name"],
            fields.get("description"),
            fields.get("enabled", True),
            json.dumps(fields.get("metrics") or []),
            json.dumps(fields.get("custom_judges") or {}),
            fields.get("sample_rate", 10),
            fields.get("span_scope", "root"),
            json.dumps(fields.get("integrations") or []),
            json.dumps(fields.get("models") or []),
            fields.get("judge"),
        )
    invalidate(org_id)
    return _row_to_rule(row)


async def update_rule(
    rule_id: uuid.UUID, org_id: uuid.UUID, **fields: Any
) -> Optional[Dict[str, Any]]:
    """Patch a rule. Only the fields present are written."""
    sets = ["updated_at = NOW()"]
    args: List[Any] = []
    idx = 1

    for column in ("name", "description", "enabled", "sample_rate", "span_scope", "judge"):
        if column in fields:
            sets.append(f"{column} = ${idx}")
            args.append(fields[column])
            idx += 1
    for column in ("metrics", "custom_judges", "integrations", "models"):
        if column in fields:
            sets.append(f"{column} = ${idx}::jsonb")
            args.append(json.dumps(fields[column]))
            idx += 1

    args += [rule_id, org_id]
    async with postgres_client.acquire() as conn:
        row = await conn.fetchrow(
            f"""
            UPDATE online_scoring_rules SET {', '.join(sets)}
            WHERE rule_id = ${idx} AND org_id = ${idx + 1}
            RETURNING *
            """,
            *args,
        )
    invalidate(org_id)
    return _row_to_rule(row) if row else None


async def delete_rule(rule_id: uuid.UUID, org_id: uuid.UUID) -> bool:
    async with postgres_client.acquire() as conn:
        result = await conn.execute(
            "DELETE FROM online_scoring_rules WHERE rule_id = $1 AND org_id = $2",
            rule_id, org_id,
        )
    invalidate(org_id)
    return result == "DELETE 1"


__all__ = [
    "CACHE_TTL_SECONDS",
    "active_rules",
    "create_rule",
    "delete_rule",
    "invalidate",
    "list_rules",
    "update_rule",
]
