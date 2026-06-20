"""Kafka consumer that turns enriched eval / security events into Slack alerts.

Distinct from the SSE :class:`TraceConsumer` in two ways:

  1. It uses a **stable shared** ``group_id`` (``KAFKA_ALERTS_GROUP_ID``) so each
     enriched event is handled by exactly one API replica — an alert must fire
     once, not once-per-replica.
  2. It joins per-org alert settings (Postgres, 60s cached) and POSTs to the
     org's Slack webhook. The producer keys enriched events by ``organization_id``,
     so all of an org's events land on one partition → one consumer instance,
     which keeps the failure-rate rolling window and digest buffer consistent.

Everything here fails open: a bad webhook, a missing config row, or a malformed
payload is logged and skipped, never raised into the consume loop.
"""
from __future__ import annotations

import asyncio
import json
import logging
import time
import uuid
from collections import defaultdict, deque
from typing import Optional

from aiokafka import AIOKafkaConsumer

import config
from db_queues.postgresql.alerts import AlertSettings, get_settings
from shared import slack

logger = logging.getLogger(__name__)

# Maps an enriched security payload's detection flags to the category ids the
# user picks in the dashboard. Empty selection = alert on any of these.
_SECURITY_FLAG_TO_CATEGORY = [
    ("injection_detected",          "prompt_injection"),
    ("jailbreak_detected",          "jailbreak"),
    ("secrets_detected",            "secrets_detected"),
    ("indirect_injection_detected", "indirect_injection"),
]

# Workers publish prefixed metric names (e.g. "ragas.faithfulness"); the
# dashboard's watched-metric ids are bare ("faithfulness"). Strip known
# prefixes so the user's selection matches what actually arrives.
def _normalize_metric(metric: str) -> str:
    m = (metric or "").lower()
    for prefix in ("ragas.", "fluiq.", "ragas_"):
        if m.startswith(prefix):
            m = m[len(prefix):]
            break
    return m


# Failure-rate window: keep the last N pass/fail outcomes per (org, metric).
_RATE_WINDOW = 30
_RATE_MIN_SAMPLES = 10
_RATE_COOLDOWN = 600.0  # seconds between failure-rate alerts for the same metric


def _dashboard_url(trace_id: Optional[str]) -> Optional[str]:
    base = config.FRONTEND_BASE_URL
    if not base:
        return None
    if trace_id:
        return f"{base}/dashboard/traces?trace={trace_id}"
    return f"{base}/dashboard/traces"


class _DigestBuffer:
    """Per-org accumulation of alert summaries flushed on an interval."""

    _INTERVALS = {"hourly": 3600.0, "daily": 86400.0}

    def __init__(self) -> None:
        # org_id -> {"webhook": str, "interval": float, "items": list, "next": float}
        self._buckets: dict[str, dict] = {}

    def add(self, org_id: str, webhook: str, digest: str, summary: str, kind: str) -> None:
        interval = self._INTERVALS.get(digest)
        if interval is None:
            return
        bucket = self._buckets.get(org_id)
        if bucket is None:
            bucket = {"webhook": webhook, "interval": interval, "items": [], "next": time.monotonic() + interval}
            self._buckets[org_id] = bucket
        bucket["webhook"] = webhook
        bucket["interval"] = interval
        bucket["items"].append({"kind": kind, "summary": summary})

    async def flush_due(self) -> None:
        now = time.monotonic()
        for org_id, bucket in list(self._buckets.items()):
            if now < bucket["next"]:
                continue
            items = bucket["items"]
            bucket["items"] = []
            bucket["next"] = now + bucket["interval"]
            if not items:
                continue
            blocks, text = slack.build_digest(items)
            await slack.post_webhook(bucket["webhook"], blocks, text)
            logger.info("[ALERTS] Flushed digest org=%s items=%d", org_id, len(items))


class AlertConsumer:
    def __init__(
        self,
        bootstrap_servers: str = config.KAFKA_BOOTSTRAP_SERVERS,
        topic: str = config.KAFKA_TRACE_PERSISTED_TOPIC,
        group_id: str = config.KAFKA_ALERTS_GROUP_ID,
    ) -> None:
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.group_id = group_id
        self._consumer: Optional[AIOKafkaConsumer] = None
        self._task: Optional[asyncio.Task] = None
        self._flush_task: Optional[asyncio.Task] = None
        self._digest = _DigestBuffer()
        self._eval_history: dict[tuple[str, str], deque] = defaultdict(lambda: deque(maxlen=_RATE_WINDOW))
        self._last_rate_alert: dict[tuple[str, str], float] = {}

    async def start(self) -> None:
        if self._consumer is not None:
            return
        if not self.bootstrap_servers or not self.topic:
            logger.warning("[ALERTS] Missing Kafka config; alert consumer disabled")
            return
        self._consumer = AIOKafkaConsumer(
            self.topic,
            bootstrap_servers=self.bootstrap_servers,
            group_id=self.group_id,
            value_deserializer=lambda b: json.loads(b.decode("utf-8")),
            enable_auto_commit=True,
            auto_offset_reset="latest",
            max_partition_fetch_bytes=config.KAFKA_MAX_FETCH_BYTES,
            **config.kafka_auth_kwargs(),
        )
        await self._consumer.start()
        self._task = asyncio.create_task(self._run(), name="alert-consumer")
        self._flush_task = asyncio.create_task(self._flush_loop(), name="alert-digest-flush")
        logger.info(
            "[ALERTS] Consumer started topic=%s group=%s servers=%s",
            self.topic, self.group_id, self.bootstrap_servers,
        )

    async def stop(self) -> None:
        for task in (self._task, self._flush_task):
            if task is not None:
                task.cancel()
                try:
                    await task
                except (asyncio.CancelledError, Exception):
                    pass
        self._task = None
        self._flush_task = None
        if self._consumer is not None:
            try:
                await self._consumer.stop()
            except Exception:
                logger.exception("[ALERTS] Error stopping consumer")
            self._consumer = None
        await slack.aclose()
        logger.info("[ALERTS] Consumer stopped group=%s", self.group_id)

    async def _flush_loop(self) -> None:
        try:
            while True:
                await asyncio.sleep(60)
                try:
                    await self._digest.flush_due()
                except Exception:
                    logger.exception("[ALERTS] Digest flush failed")
        except asyncio.CancelledError:
            raise

    async def _run(self) -> None:
        assert self._consumer is not None
        try:
            async for msg in self._consumer:
                payload = msg.value
                if not isinstance(payload, dict) or payload.get("kind") != "enriched":
                    continue
                org_id = payload.get("organization_id")
                if not org_id:
                    continue
                try:
                    await self._handle(str(org_id), payload)
                except Exception:
                    logger.exception("[ALERTS] Failed to handle enriched event org=%s", org_id)
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("[ALERTS] Consumer loop crashed; will stop")

    async def _handle(self, org_id: str, payload: dict) -> None:
        enrichment = payload.get("enrichment")
        if enrichment == "evaluation":
            await self._handle_eval(org_id, payload)
        elif enrichment == "security":
            await self._handle_security(org_id, payload)

    async def _settings(self, org_id: str) -> Optional[AlertSettings]:
        try:
            s = await get_settings(uuid.UUID(org_id))
        except Exception:
            logger.exception("[ALERTS] Could not load settings org=%s", org_id)
            return None
        return s if s.slack_webhook else None

    async def _deliver(self, settings: AlertSettings, blocks: list, text: str, summary: str, kind: str) -> None:
        """Send now (realtime) or buffer for the next digest flush.

        Awaited rather than fire-and-forget: alerting events are a small slice of
        total traffic, so the brief per-event block is cheap and gives natural
        backpressure (and avoids dangling-task GC warnings)."""
        if settings.digest == "realtime":
            await slack.post_webhook(settings.slack_webhook, blocks, text)
        else:
            self._digest.add(settings.org_id, settings.slack_webhook, settings.digest, summary, kind)

    async def _handle_eval(self, org_id: str, payload: dict) -> None:
        ev = payload.get("evaluation") or {}
        raw_metric = ev.get("metric")
        score = ev.get("score")
        if raw_metric is None or score is None:
            return
        metric = _normalize_metric(raw_metric)
        settings = await self._settings(org_id)
        if settings is None or not settings.eval_enabled:
            return
        # Empty metric selection = watch all metrics.
        if settings.eval_metrics and metric not in settings.eval_metrics:
            return

        score = float(score)
        details = ev.get("details") or {}
        passed = details.get("passed")
        failed = (not passed) if isinstance(passed, bool) else (score < settings.eval_score_below)

        trace_id = payload.get("trace_id")

        # 1) Per-event threshold breach.
        if score < settings.eval_score_below:
            blocks, text = slack.build_eval_alert(
                metric=metric, score=score, threshold=settings.eval_score_below,
                trace_id=trace_id, dashboard_url=_dashboard_url(trace_id),
            )
            await self._deliver(settings, blocks, text,
                                summary=f"Eval `{metric}` scored {score:.2f} (< {settings.eval_score_below:.2f})",
                                kind="eval")

        # 2) Rolling failure-rate breach (debounced).
        key = (org_id, metric)
        hist = self._eval_history[key]
        hist.append(failed)
        if len(hist) >= _RATE_MIN_SAMPLES:
            rate = 100.0 * sum(1 for f in hist if f) / len(hist)
            if rate > settings.eval_failure_rate_above:
                last = self._last_rate_alert.get(key, 0.0)
                if time.monotonic() - last >= _RATE_COOLDOWN:
                    self._last_rate_alert[key] = time.monotonic()
                    text = f"Eval failure rate for {metric} is {rate:.0f}% (> {settings.eval_failure_rate_above:.0f}%)"
                    blocks = [
                        {"type": "section", "text": {"type": "mrkdwn",
                         "text": f"⚠️ *Eval failure rate high*\n*Metric:* `{metric}`\n*Failure rate:* `{rate:.0f}%` over last {len(hist)} evals (threshold `{settings.eval_failure_rate_above:.0f}%`)"}},
                        {"type": "context", "elements": [{"type": "mrkdwn", "text": "via Fluiq"}]},
                    ]
                    await self._deliver(settings, blocks, text,
                                        summary=f"Eval `{metric}` failure rate {rate:.0f}%",
                                        kind="eval")

    async def _handle_security(self, org_id: str, payload: dict) -> None:
        sec = payload.get("security") or {}
        risk = sec.get("security_risk_level") or "clean"
        should_block = bool(sec.get("should_block"))
        settings = await self._settings(org_id)
        if settings is None or not settings.security_enabled:
            return

        # Derive which user-facing categories this event triggered.
        categories: list[str] = []
        for flag, cat in _SECURITY_FLAG_TO_CATEGORY:
            if sec.get(flag):
                categories.append(cat)
        if sec.get("pii_entities_prompt") or sec.get("pii_entities_response"):
            categories.append("pii_detected")

        # Filters: blocked-only, risk level, category subset.
        if settings.security_blocked_only and not should_block:
            return
        if risk not in settings.security_alert_on:
            return
        if settings.security_categories:
            if not any(c in settings.security_categories for c in categories):
                return

        trace_id = payload.get("trace_id")
        blocks, text = slack.build_security_alert(
            risk_level=risk, attack_types=categories, should_block=should_block,
            trace_id=trace_id, dashboard_url=_dashboard_url(trace_id),
        )
        await self._deliver(settings, blocks, text,
                            summary=f"Security {('blocked' if should_block else 'flagged')}: {risk} ({', '.join(categories) or 'unknown'})",
                            kind="security")


alert_consumer = AlertConsumer()

__all__ = ["AlertConsumer", "alert_consumer"]
