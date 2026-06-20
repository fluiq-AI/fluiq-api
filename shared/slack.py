"""Slack Incoming Webhook delivery for the alert dispatcher.

Pure formatting + POST. No DB, no Kafka — callers decide *when* to alert; this
module only knows *how* to render and ship a message. Failures are swallowed
and logged: a flaky webhook must never break trace processing.
"""
from __future__ import annotations

import logging
from typing import Any, Optional

import httpx

logger = logging.getLogger(__name__)

_TIMEOUT = httpx.Timeout(5.0, connect=3.0)

# Slack's hard ceiling is ~1s rate per webhook; we keep one shared client.
_client: Optional[httpx.AsyncClient] = None


def _get_client() -> httpx.AsyncClient:
    global _client
    if _client is None:
        _client = httpx.AsyncClient(timeout=_TIMEOUT)
    return _client


async def aclose() -> None:
    global _client
    if _client is not None:
        await _client.aclose()
        _client = None


_RISK_EMOJI = {"low": "🟡", "medium": "🟠", "high": "🔴", "clean": "🟢"}


async def post_webhook(webhook_url: str, blocks: list[dict], text: str) -> bool:
    """POST a Block Kit message. Returns True on a 2xx, False otherwise."""
    if not webhook_url:
        return False
    try:
        resp = await _get_client().post(webhook_url, json={"text": text, "blocks": blocks})
        if resp.status_code // 100 == 2:
            return True
        logger.warning("[ALERTS] Slack webhook returned %s: %s", resp.status_code, resp.text[:200])
        return False
    except Exception:
        logger.exception("[ALERTS] Slack webhook delivery failed")
        return False


def _section(text: str) -> dict:
    return {"type": "section", "text": {"type": "mrkdwn", "text": text}}


def _context(text: str) -> dict:
    return {"type": "context", "elements": [{"type": "mrkdwn", "text": text}]}


# ── Message builders ────────────────────────────────────────────────────────────

def build_eval_alert(
    *, metric: str, score: float, threshold: float,
    trace_id: Optional[str], dashboard_url: Optional[str],
) -> tuple[list[dict], str]:
    text = f"Eval regression: {metric} scored {score:.2f} (below {threshold:.2f})"
    blocks: list[dict] = [
        _section(f"⚠️ *Eval regression detected*"),
        _section(
            f"*Metric:* `{metric}`\n"
            f"*Score:* `{score:.3f}`  •  *Threshold:* `{threshold:.2f}`"
        ),
    ]
    meta = []
    if trace_id:
        meta.append(f"trace `{trace_id}`")
    if dashboard_url:
        blocks.append(_section(f"<{dashboard_url}|View in Fluiq →>"))
    if meta:
        blocks.append(_context("  •  ".join(meta) + "  •  via Fluiq"))
    else:
        blocks.append(_context("via Fluiq"))
    return blocks, text


def build_security_alert(
    *, risk_level: str, attack_types: list[str], should_block: bool,
    trace_id: Optional[str], dashboard_url: Optional[str],
) -> tuple[list[dict], str]:
    emoji = _RISK_EMOJI.get(risk_level, "⚪")
    action = "blocked" if should_block else "flagged"
    cats = ", ".join(f"`{a}`" for a in attack_types) or "`unknown`"
    text = f"Security {action}: {risk_level} risk ({', '.join(attack_types) or 'unknown'})"
    blocks: list[dict] = [
        _section(f"{emoji} *Security event {action}*"),
        _section(f"*Risk level:* `{risk_level}`\n*Categories:* {cats}"),
    ]
    if dashboard_url:
        blocks.append(_section(f"<{dashboard_url}|View in Fluiq →>"))
    blocks.append(_context((f"trace `{trace_id}`  •  " if trace_id else "") + "via Fluiq"))
    return blocks, text


def build_test_message() -> tuple[list[dict], str]:
    return (
        [
            _section("✅ *Fluiq alerts are connected*"),
            _section("This is a test message. Real alerts will appear here when your eval or security thresholds are crossed."),
            _context("via Fluiq"),
        ],
        "Fluiq alerts are connected — this is a test message.",
    )


def build_digest(items: list[dict]) -> tuple[list[dict], str]:
    """Render a batched digest of buffered alert items.

    Each item is ``{"kind": "eval"|"security", "summary": str}``.
    """
    n = len(items)
    blocks: list[dict] = [_section(f"📊 *Fluiq alert digest* — {n} event{'s' if n != 1 else ''}")]
    lines = [f"• {it['summary']}" for it in items[:40]]
    if lines:
        blocks.append(_section("\n".join(lines)))
    if n > 40:
        blocks.append(_context(f"…and {n - 40} more"))
    else:
        blocks.append(_context("via Fluiq"))
    return blocks, f"Fluiq alert digest — {n} events"
