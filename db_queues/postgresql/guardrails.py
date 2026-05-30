"""Guardrail policy CRUD with a 60-second in-process cache.

Each org can have multiple named policies (slugs). "default" is always the
fallback. get_policy(org_id, slug) tries the requested slug first, then falls
back to "default", then falls back to an in-memory default.
"""
from __future__ import annotations

import time
import uuid
from dataclasses import dataclass, field
from typing import Optional

import config
from . import postgres_client

_TABLE = config.POSTGRES_GUARDRAILS_TABLE

ALL_CATEGORIES = [
    "prompt_injection",
    "jailbreak",
    "skeleton_key",
    "semantic_attack",
    "pii_detected",
    "secrets_detected",
    "indirect_injection",
]


@dataclass
class GuardrailPolicy:
    org_id:            str
    slug:              str                = "default"
    block_threshold:   str                = "high"
    warn_threshold:    str                = "medium"
    block_categories:  list[str]          = field(default_factory=list)
    custom_deny_list:  list[str]          = field(default_factory=list)
    custom_allow_list: list[str]          = field(default_factory=list)
    alert_webhook:     Optional[str]      = None
    alert_on:          list[str]          = field(default_factory=lambda: ["high"])
    scan_responses:    bool               = False

    def to_dict(self) -> dict:
        return {
            "org_id":            self.org_id,
            "slug":              self.slug,
            "block_threshold":   self.block_threshold,
            "warn_threshold":    self.warn_threshold,
            "block_categories":  self.block_categories,
            "custom_deny_list":  self.custom_deny_list,
            "custom_allow_list": self.custom_allow_list,
            "alert_webhook":     self.alert_webhook,
            "alert_on":          self.alert_on,
            "scan_responses":    self.scan_responses,
        }


# Cache keyed by "org_id:slug"
_CACHE: dict[str, tuple[GuardrailPolicy, float]] = {}
_TTL = 60.0


def _default(org_id: str, slug: str = "default") -> GuardrailPolicy:
    return GuardrailPolicy(org_id=org_id, slug=slug)


def _from_row(row, org_id: str) -> GuardrailPolicy:
    return GuardrailPolicy(
        org_id            = org_id,
        slug              = row["slug"],
        block_threshold   = row["block_threshold"],
        warn_threshold    = row["warn_threshold"],
        block_categories  = list(row["block_categories"] or []),
        custom_deny_list  = list(row["custom_deny_list"] or []),
        custom_allow_list = list(row["custom_allow_list"] or []),
        alert_webhook     = row["alert_webhook"],
        alert_on          = list(row["alert_on"] or ["high"]),
        scan_responses    = bool(row["scan_responses"]),
    )


async def get_policy(org_id: uuid.UUID, slug: str = "default") -> GuardrailPolicy:
    key = f"{org_id}:{slug}"
    cached, ts = _CACHE.get(key, (None, 0.0))
    if cached is not None and (time.monotonic() - ts) < _TTL:
        return cached

    async with postgres_client.acquire() as conn:
        row = await conn.fetchrow(
            f"SELECT * FROM {_TABLE} WHERE org_id = $1 AND slug = $2", org_id, slug
        )
        if row is None and slug != "default":
            row = await conn.fetchrow(
                f"SELECT * FROM {_TABLE} WHERE org_id = $1 AND slug = 'default'", org_id
            )

    policy = _from_row(row, str(org_id)) if row else _default(str(org_id), slug)
    _CACHE[key] = (policy, time.monotonic())
    return policy


async def list_slugs(org_id: uuid.UUID) -> list[str]:
    async with postgres_client.acquire() as conn:
        rows = await conn.fetch(
            f"SELECT slug FROM {_TABLE} WHERE org_id = $1 ORDER BY slug", org_id
        )
    slugs = [row["slug"] for row in rows]
    return slugs if slugs else ["default"]


async def upsert_policy(org_id: uuid.UUID, policy: GuardrailPolicy) -> GuardrailPolicy:
    async with postgres_client.acquire() as conn:
        await conn.execute(
            f"""
            INSERT INTO {_TABLE}
                (org_id, slug, block_threshold, warn_threshold, block_categories,
                 custom_deny_list, custom_allow_list, alert_webhook, alert_on,
                 scan_responses, updated_at)
            VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,NOW())
            ON CONFLICT (org_id, slug) DO UPDATE SET
                block_threshold   = EXCLUDED.block_threshold,
                warn_threshold    = EXCLUDED.warn_threshold,
                block_categories  = EXCLUDED.block_categories,
                custom_deny_list  = EXCLUDED.custom_deny_list,
                custom_allow_list = EXCLUDED.custom_allow_list,
                alert_webhook     = EXCLUDED.alert_webhook,
                alert_on          = EXCLUDED.alert_on,
                scan_responses    = EXCLUDED.scan_responses,
                updated_at        = NOW()
            """,
            org_id,
            policy.slug,
            policy.block_threshold,
            policy.warn_threshold,
            policy.block_categories,
            policy.custom_deny_list,
            policy.custom_allow_list,
            policy.alert_webhook,
            policy.alert_on,
            policy.scan_responses,
        )
    _CACHE[f"{org_id}:{policy.slug}"] = (policy, time.monotonic())
    return policy


async def delete_policy(org_id: uuid.UUID, slug: str) -> None:
    if slug == "default":
        raise ValueError("Cannot delete the default policy")
    async with postgres_client.acquire() as conn:
        await conn.execute(
            f"DELETE FROM {_TABLE} WHERE org_id = $1 AND slug = $2", org_id, slug
        )
    _CACHE.pop(f"{org_id}:{slug}", None)
