"""Background Kafka consumer that resolves per-request asyncio Futures.

The /secure/check endpoint publishes a security_check_sync job to the eval
topic and then awaits a Future keyed by correlation_id.  When the evaluator
worker finishes the full scan it publishes the result to the reply topic, and
this consumer resolves the matching Future so the HTTP response can return.

Each API instance uses a unique group_id so it receives all reply messages —
replies published for requests handled by other instances are silently skipped
(their correlation_id won't be in _pending)
"""
from __future__ import annotations

import asyncio
import json
import logging
from typing import Any, Optional
from uuid import uuid4

from aiokafka import AIOKafkaConsumer
import config

logger = logging.getLogger(__name__)

# correlation_id -> (future, expected_org_id | None). The expected org lets the
# consumer reject a reply carrying a mismatched org_id — defense in depth against
# a worker echoing the wrong correlation_id and leaking one tenant's verdict to
# another. Verification is skipped when either side omits the org (e.g. the
# response-gate path), preserving correlation-id-only matching there.
_pending: dict[str, tuple[asyncio.Future, Optional[str]]] = {}


class SecurityReplyConsumer:
    def __init__(self, 
                bootstrap_servers: str = config.KAFKA_BOOTSTRAP_SERVERS,
                default_topic: str = config.KAFKA_SECURITY_REPLY_TOPIC,
                 ) -> None:
        self._consumer: Optional[AIOKafkaConsumer] = None
        self._task: Optional[asyncio.Task] = None
        self.bootstrap_servers = bootstrap_servers
        self.default_topic = default_topic
        
    async def start(self) -> None:
        if not config.KAFKA_SECURITY_REPLY_TOPIC:
            logger.warning("[KAFKA] KAFKA_SECURITY_REPLY_TOPIC not set — reply consumer disabled")
            return
        
        self._consumer = AIOKafkaConsumer(
            self.default_topic,
            bootstrap_servers=self.bootstrap_servers,
            group_id=f"fluiq-api-security-reply-{uuid4().hex}",
            value_deserializer=lambda b: json.loads(b.decode("utf-8")),
            auto_offset_reset="latest",
            enable_auto_commit=False,
            **config.kafka_auth_kwargs(),
        )
        await self._consumer.start()
        self._task = asyncio.create_task(self._consume())
        logger.info("[KAFKA] Security reply consumer started topic=%s", config.KAFKA_SECURITY_REPLY_TOPIC)

    async def _consume(self) -> None:
        try:
            async for msg in self._consumer:
                correlation_id = msg.value.get("correlation_id")
                if not correlation_id:
                    continue
                entry = _pending.get(correlation_id)
                if not entry:
                    continue
                fut, expected_org = entry
                if fut.done():
                    continue
                reply_org = msg.value.get("org_id")
                if expected_org and reply_org and str(reply_org) != str(expected_org):
                    logger.warning("[KAFKA] Dropping security reply with mismatched org_id")
                    continue
                fut.set_result(msg.value.get("result"))
        except asyncio.CancelledError:
            pass
        except Exception:
            logger.exception("[KAFKA] Security reply consumer error")
        finally:
            if self._consumer:
                await self._consumer.stop()

    async def stop(self) -> None:
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass


security_reply_consumer = SecurityReplyConsumer()


async def wait_for_reply(
    correlation_id: str,
    timeout: float,
    expected_org: Optional[str] = None,
) -> Optional[dict[str, Any]]:
    """Register the correlation_id slot, await the worker reply, clean up on
    timeout. When ``expected_org`` is given, a reply whose ``org_id`` doesn't
    match is rejected by the consumer."""
    loop = asyncio.get_running_loop()
    fut: asyncio.Future = loop.create_future()
    _pending[correlation_id] = (fut, expected_org)
    try:
        return await asyncio.wait_for(fut, timeout=timeout)
    except (asyncio.TimeoutError, asyncio.CancelledError):
        return None
    finally:
        _pending.pop(correlation_id, None)


# ── Playground reply consumer ──────────────────────────────────────────────────

_playground_pending: dict[str, asyncio.Future] = {}


class PlaygroundReplyConsumer:
    """Resolves per-request Futures for playground evaluation replies.

    The playground endpoint publishes a ``playground_eval`` job to the eval
    topic with a ``correlation_id``. The worker processes the eval and
    publishes the result to ``KAFKA_PLAYGROUND_REPLY_TOPIC``. This consumer
    resolves the matching Future so the HTTP response can return.
    """

    def __init__(
            self,
            bootstrap_servers: str = config.KAFKA_BOOTSTRAP_SERVERS,
            default_topic: str = config.KAFKA_PLAYGROUND_REPLY_TOPIC,
            ) -> None:
        self._consumer: Optional[AIOKafkaConsumer] = None
        self._task: Optional[asyncio.Task] = None
        self.bootstrap_servers = bootstrap_servers
        self.default_topic = default_topic

    async def start(self) -> None:
        if not config.KAFKA_PLAYGROUND_REPLY_TOPIC:
            logger.warning("[KAFKA] KAFKA_PLAYGROUND_REPLY_TOPIC not set — playground reply consumer disabled")
            return

        self._consumer = AIOKafkaConsumer(
            self.default_topic,
            bootstrap_servers=self.bootstrap_servers,
            group_id=f"fluiq-api-playground-reply-{uuid4().hex}",
            value_deserializer=lambda b: json.loads(b.decode("utf-8")),
            auto_offset_reset="latest",
            enable_auto_commit=False,
            **config.kafka_auth_kwargs(),
        )
        await self._consumer.start()
        self._task = asyncio.create_task(self._consume())
        logger.info("[KAFKA] Playground reply consumer started topic=%s", config.KAFKA_PLAYGROUND_REPLY_TOPIC)

    async def _consume(self) -> None:
        try:
            async for msg in self._consumer:
                correlation_id = msg.value.get("correlation_id")
                if not correlation_id:
                    continue
                fut = _playground_pending.get(correlation_id)
                if fut and not fut.done():
                    fut.set_result(msg.value.get("result"))
        except asyncio.CancelledError:
            pass
        except Exception:
            logger.exception("[KAFKA] Playground reply consumer error")
        finally:
            if self._consumer:
                await self._consumer.stop()

    async def stop(self) -> None:
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass


playground_reply_consumer = PlaygroundReplyConsumer()


async def wait_for_playground_reply(correlation_id: str, timeout: float) -> Optional[dict[str, Any]]:
    """Register a Future for this correlation_id and await the worker reply."""
    loop = asyncio.get_running_loop()
    fut: asyncio.Future = loop.create_future()
    _playground_pending[correlation_id] = fut
    try:
        return await asyncio.wait_for(fut, timeout=timeout)
    except (asyncio.TimeoutError, asyncio.CancelledError):
        return None
    finally:
        _playground_pending.pop(correlation_id, None)
