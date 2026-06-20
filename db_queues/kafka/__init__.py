import json
import config
import logging
from typing import Any, Optional
from aiokafka import AIOKafkaProducer

logger = logging.getLogger(__name__)

class KafkaQueue:
    """Async Kafka producer wrapper for enqueuing trace jobs."""

    def __init__(
        self,
        bootstrap_servers: str = config.KAFKA_BOOTSTRAP_SERVERS,
        default_topic: str = config.KAFKA_TRACE_TOPIC,
    ) -> None:

        self.bootstrap_servers = bootstrap_servers
        self.default_topic = default_topic
        self._producer: Optional[AIOKafkaProducer] = None

    async def start(self) -> None:
        if self._producer is not None:
            return

        self._producer = AIOKafkaProducer(
            bootstrap_servers=self.bootstrap_servers,
            value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
            key_serializer=lambda k: k.encode("utf-8") if isinstance(k, str) else k,
            acks="all",
            enable_idempotence=True,
            # Allow multi-MB trace events; gzip shrinks the on-wire batch the
            # broker sees (the max_request_size guard itself runs pre-compression).
            max_request_size=config.KAFKA_MAX_REQUEST_SIZE,
            compression_type="gzip",
            **config.kafka_auth_kwargs(),
        )
        await self._producer.start()
        logger.info("[KAFKA] Kafka producer started: %s", self.bootstrap_servers)

    async def stop(self) -> None:
        if self._producer is None:
            return
        await self._producer.stop()
        self._producer = None
        logger.info("[KAFKA] Kafka producer stopped")

    async def add_job(
        self,
        job: dict[str, Any],
        topic: Optional[str] = None,
        key: Optional[str] = None,
    ) -> None:
        if self._producer is None:
            await self.start()
        await self._producer.send_and_wait(
            topic or self.default_topic,
            value=job,
            key=key,
        )


kafka_queue = KafkaQueue()

from db_queues.kafka.reply_consumer import (
    security_reply_consumer,
    wait_for_reply,
    playground_reply_consumer,
    wait_for_playground_reply,
)

__all__ = [
    "KafkaQueue", "kafka_queue",
    "security_reply_consumer", "wait_for_reply",
    "playground_reply_consumer", "wait_for_playground_reply",
]
