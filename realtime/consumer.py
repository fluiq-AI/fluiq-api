"""Kafka consumer that bridges `traces.persisted` into the in-process
:class:`TraceBroker` for SSE fan-out.

Each API replica runs a single instance with a *unique* group_id (UUID
suffix) so every replica receives every message — the broker then fans
each event out to whichever SSE connections happen to live on this
replica. Offsets are not committed: this stream is purely for live
notifications, replay would deliver stale traces to clients that
already saw them.
"""
import asyncio
import json
import logging
import config
import uuid
from typing import Optional

from aiokafka import AIOKafkaConsumer
from .broker import trace_broker
from .running_registry import running_registry

logger = logging.getLogger(__name__)


class TraceConsumer:
    def __init__(
        self,
        bootstrap_servers: str = config.KAFKA_BOOTSTRAP_SERVERS,
        topic: str = config.KAFKA_TRACE_PERSISTED_TOPIC,
    ) -> None:
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self._consumer: Optional[AIOKafkaConsumer] = None
        self._task: Optional[asyncio.Task] = None
        self._group_id = f"api-sse-{uuid.uuid4()}"

    async def start(self) -> None:
        if self._consumer is not None:
            return
        
        self._consumer = AIOKafkaConsumer(
            self.topic,
            bootstrap_servers=self.bootstrap_servers,
            group_id=self._group_id,
            value_deserializer=lambda b: json.loads(b.decode("utf-8")),
            enable_auto_commit=False,
            auto_offset_reset="latest",
            max_partition_fetch_bytes=config.KAFKA_MAX_FETCH_BYTES,
            **config.kafka_auth_kwargs(),
        )
        await self._consumer.start()
        self._task = asyncio.create_task(self._run(), name="trace-sse-consumer")
        logger.info(
            "[SSE] Consumer started topic=%s group=%s servers=%s",
            self.topic, self._group_id, self.bootstrap_servers,
        )

    async def stop(self) -> None:
        if self._task is not None:
            self._task.cancel()
            try:
                await self._task
            except (asyncio.CancelledError, Exception):
                pass
            self._task = None
        if self._consumer is not None:
            try:
                await self._consumer.stop()
            except Exception:
                logger.exception("[SSE] Error stopping consumer")
            self._consumer = None
        logger.info("[SSE] Consumer stopped group=%s", self._group_id)

    async def _run(self) -> None:
        assert self._consumer is not None
        try:
            async for msg in self._consumer:
                payload = msg.value
                if not isinstance(payload, dict):
                    continue
                org_id = payload.get("organization_id")
                if not org_id:
                    continue
                # Mirror started/completed events into the running registry
                # so a refresh-during-run page load surfaces in-flight rows
                # via /api/v1/traces. ``persisted`` and ``enriched`` are
                # both signals that the durable row has landed (or its
                # cost/eval enrichment has) — either evicts the placeholder.
                kind = payload.get("kind")
                try:
                    if kind == "started":
                        await running_registry.register(payload)
                    elif kind in ("persisted", "enriched"):
                        await running_registry.complete(payload)
                except Exception:
                    logger.exception(
                        "[SSE] Running-registry update failed org=%s kind=%s",
                        org_id, kind,
                    )
                try:
                    await trace_broker.publish(str(org_id), payload)
                except Exception:
                    logger.exception(
                        "[SSE] Failed to dispatch trace org=%s", org_id,
                    )
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("[SSE] Consumer loop crashed; will stop")


trace_consumer = TraceConsumer()

__all__ = [
    "TraceConsumer",
    "trace_consumer",
    "KAFKA_TRACE_PERSISTED_TOPIC",
]
