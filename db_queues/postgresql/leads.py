"""Marketing lead capture for the public site.

Strangers, not users — there is no org_id here. A lead is an email plus the
page that produced it, so we can tell which of the public pages (the
response-gate demo, the cost calculator, the ``*-alternative`` comparison
pages) actually converts.

Writes are idempotent per (email, source_page): a repeat submit keeps the
original ``created_at`` so first-seen is never overwritten.
"""
from __future__ import annotations

import logging
import re
import uuid
from typing import Any, Optional

import config
from . import postgres_client

logger = logging.getLogger(__name__)

_TABLE = config.POSTGRES_LEADS_TABLE

# Deliberately permissive: this is a lead form, not an auth boundary. It only
# has to reject obvious junk so the table stays worth reading by hand.
_EMAIL_RE = re.compile(r"^[^@\s]+@[^@\s.]+\.[^@\s]{2,}$")

MAX_EMAIL_LEN  = 254
MAX_SOURCE_LEN = 200
MAX_TEXT_LEN   = 500


def is_valid_email(email: str) -> bool:
    return bool(email) and len(email) <= MAX_EMAIL_LEN and _EMAIL_RE.match(email) is not None


def _clip(value: Optional[str], limit: int) -> Optional[str]:
    if value is None:
        return None
    value = value.strip()
    return value[:limit] or None


async def capture(
    *,
    email: str,
    source_page: str,
    context: Optional[dict[str, Any]] = None,
    referrer: Optional[str] = None,
    user_agent: Optional[str] = None,
) -> bool:
    """Persist a lead. Returns True when this is a new (email, source_page)."""
    email       = email.strip().lower()
    source_page = (_clip(source_page, MAX_SOURCE_LEN) or "unknown")

    async with postgres_client.acquire() as conn:
        row = await conn.fetchrow(
            f"""
            INSERT INTO {_TABLE} (lead_id, email, source_page, context, referrer, user_agent)
            VALUES ($1, $2, $3, $4, $5, $6)
            ON CONFLICT (lower(email), source_page) DO NOTHING
            RETURNING lead_id
            """,
            uuid.uuid4(),
            email,
            source_page,
            context or {},
            _clip(referrer, MAX_TEXT_LEN),
            _clip(user_agent, MAX_TEXT_LEN),
        )
    return row is not None


async def list_leads(*, limit: int = 100, offset: int = 0) -> list[dict[str, Any]]:
    async with postgres_client.acquire() as conn:
        rows = await conn.fetch(
            f"""
            SELECT lead_id, email, source_page, context, referrer, created_at
              FROM {_TABLE}
             ORDER BY created_at DESC
             LIMIT $1 OFFSET $2
            """,
            limit,
            offset,
        )
    return [
        {
            "lead_id":     str(r["lead_id"]),
            "email":       r["email"],
            "source_page": r["source_page"],
            "context":     r["context"],
            "referrer":    r["referrer"],
            "created_at":  r["created_at"].isoformat(),
        }
        for r in rows
    ]


async def counts_by_source() -> list[dict[str, Any]]:
    """Leads per source page — the number that tells you which page works."""
    async with postgres_client.acquire() as conn:
        rows = await conn.fetch(
            f"SELECT source_page, COUNT(*) AS n FROM {_TABLE} GROUP BY source_page ORDER BY n DESC"
        )
    return [{"source_page": r["source_page"], "count": r["n"]} for r in rows]


async def total() -> int:
    async with postgres_client.acquire() as conn:
        return await conn.fetchval(f"SELECT COUNT(*) FROM {_TABLE}") or 0
