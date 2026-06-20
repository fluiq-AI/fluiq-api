"""Per-org alert settings CRUD with a 60-second in-process cache.

One row per org (no slugs). Drives the Slack alert dispatcher in
``realtime/alert_consumer.py``. ``get_settings`` falls back to an in-memory
default so a missing row never breaks the dispatcher.
"""
from __future__ import annotations

import time
import uuid
from dataclasses import dataclass, field
from typing import Optional

import config
from . import postgres_client

_TABLE = config.POSTGRES_ALERTS_TABLE

VALID_DIGESTS    = {"realtime", "hourly", "daily"}
VALID_RISK       = {"low", "medium", "high"}
EVAL_METRICS     = {"faithfulness", "answer_relevancy", "context_precision", "hallucination", "toxicity"}
SECURITY_CATEGORIES = {
    "prompt_injection", "jailbreak", "pii_detected",
    "secrets_detected", "indirect_injection",
}


@dataclass
class AlertSettings:
    org_id:                  str
    slack_webhook:           Optional[str] = None
    digest:                  str           = "realtime"
    eval_enabled:            bool          = False
    eval_metrics:            list[str]     = field(default_factory=list)
    eval_score_below:        float         = 0.7
    eval_failure_rate_above: float         = 10.0
    security_enabled:        bool          = False
    security_alert_on:       list[str]     = field(default_factory=lambda: ["high"])
    security_categories:     list[str]     = field(default_factory=list)
    security_blocked_only:   bool          = True

    def to_dict(self, *, redact_webhook: bool = False) -> dict:
        webhook = self.slack_webhook
        if redact_webhook and webhook:
            # Show only enough to confirm a webhook is set without leaking it.
            webhook = webhook[:30] + "…"
        return {
            "slack_webhook":           webhook,
            "digest":                  self.digest,
            "eval": {
                "enabled":            self.eval_enabled,
                "metrics":            self.eval_metrics,
                "score_below":        self.eval_score_below,
                "failure_rate_above": self.eval_failure_rate_above,
            },
            "security": {
                "enabled":      self.security_enabled,
                "alert_on":     self.security_alert_on,
                "categories":   self.security_categories,
                "blocked_only": self.security_blocked_only,
            },
        }


_CACHE: dict[str, tuple[AlertSettings, float]] = {}
_TTL = 60.0


def _default(org_id: str) -> AlertSettings:
    return AlertSettings(org_id=org_id)


def _from_row(row, org_id: str) -> AlertSettings:
    return AlertSettings(
        org_id                  = org_id,
        slack_webhook           = row["slack_webhook"],
        digest                  = row["digest"],
        eval_enabled            = bool(row["eval_enabled"]),
        eval_metrics            = list(row["eval_metrics"] or []),
        eval_score_below        = float(row["eval_score_below"]),
        eval_failure_rate_above = float(row["eval_failure_rate_above"]),
        security_enabled        = bool(row["security_enabled"]),
        security_alert_on       = list(row["security_alert_on"] or ["high"]),
        security_categories     = list(row["security_categories"] or []),
        security_blocked_only   = bool(row["security_blocked_only"]),
    )


async def get_settings(org_id: uuid.UUID) -> AlertSettings:
    key = str(org_id)
    cached, ts = _CACHE.get(key, (None, 0.0))
    if cached is not None and (time.monotonic() - ts) < _TTL:
        return cached

    async with postgres_client.acquire() as conn:
        row = await conn.fetchrow(f"SELECT * FROM {_TABLE} WHERE org_id = $1", org_id)

    settings = _from_row(row, key) if row else _default(key)
    _CACHE[key] = (settings, time.monotonic())
    return settings


async def upsert_settings(org_id: uuid.UUID, s: AlertSettings) -> AlertSettings:
    async with postgres_client.acquire() as conn:
        await conn.execute(
            f"""
            INSERT INTO {_TABLE}
                (org_id, slack_webhook, digest, eval_enabled, eval_metrics,
                 eval_score_below, eval_failure_rate_above, security_enabled,
                 security_alert_on, security_categories, security_blocked_only,
                 updated_at)
            VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,NOW())
            ON CONFLICT (org_id) DO UPDATE SET
                slack_webhook           = EXCLUDED.slack_webhook,
                digest                  = EXCLUDED.digest,
                eval_enabled            = EXCLUDED.eval_enabled,
                eval_metrics            = EXCLUDED.eval_metrics,
                eval_score_below        = EXCLUDED.eval_score_below,
                eval_failure_rate_above = EXCLUDED.eval_failure_rate_above,
                security_enabled        = EXCLUDED.security_enabled,
                security_alert_on       = EXCLUDED.security_alert_on,
                security_categories     = EXCLUDED.security_categories,
                security_blocked_only   = EXCLUDED.security_blocked_only,
                updated_at              = NOW()
            """,
            org_id,
            s.slack_webhook,
            s.digest,
            s.eval_enabled,
            s.eval_metrics,
            s.eval_score_below,
            s.eval_failure_rate_above,
            s.security_enabled,
            s.security_alert_on,
            s.security_categories,
            s.security_blocked_only,
        )
    _CACHE[str(org_id)] = (s, time.monotonic())
    return s
