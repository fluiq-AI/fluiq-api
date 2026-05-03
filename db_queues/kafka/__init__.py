from os import PathLike
import json
import logging
from typing import Any, Optional
from aiokafka import AIOKafkaProducer
from aiokafka.helpers import create_ssl_context
import config

logger = logging.getLogger(__name__)

class KafkaQueue:
    """Async Kafka producer wrapper for enqueuing trace jobs."""

    def __init__(
        self,
        bootstrap_servers: str = config.KAFKA_BOOTSTRAP_SERVERS,
        default_topic: str = config.KAFKA_TRACE_TOPIC,
        kafka_cafile: str | bytes | PathLike[str] | PathLike[bytes] | None = config.KAFKA_SSL_CA_FILE,
        kafka_certfile: str | bytes | PathLike[str] | PathLike[bytes] | None = config.KAFKA_SSL_CERT_FILE,
        kafka_keyfile: str | bytes | PathLike[str] | PathLike[bytes] | None = config.KAFKA_SSL_KEY_FILE,
        kafka_security_protocol: str = config.KAFKA_SECURITY_PROTOCOL
    ) -> None:

        self.bootstrap_servers = bootstrap_servers
        self.kafka_cafile = kafka_cafile
        self.kafka_certfile = kafka_certfile
        self.kafka_keyfile = kafka_keyfile
        self.kafka_security_protocol = kafka_security_protocol
        self.default_topic = default_topic
        self._producer: Optional[AIOKafkaProducer] = None

    async def start(self) -> None:
        if self._producer is not None:
            return
        
        logger.info(f"Kafka CA File: {self.kafka_cafile} | {config.KAFKA_SSL_CA_FILE}")
        logger.info(f"Kafka Cert File: {self.kafka_certfile} | {config.KAFKA_SSL_CERT_FILE}")
        logger.info(f"Kafka Key File: {self.kafka_keyfile} | {config.KAFKA_SSL_KEY_FILE}")

        context = create_ssl_context(
            cafile=self.kafka_cafile,
            certfile=self.kafka_certfile,
            keyfile=self.kafka_keyfile
        )

        self._producer = AIOKafkaProducer(
            bootstrap_servers=self.bootstrap_servers,
            security_protocol=self.kafka_security_protocol,
            ssl_context=context,
            value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
            key_serializer=lambda k: k.encode("utf-8") if isinstance(k, str) else k,
            acks="all",
            enable_idempotence=True,
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

__all__ = ["KafkaQueue", "kafka_queue", "KAFKA_BOOTSTRAP_SERVERS", "KAFKA_TRACE_TOPIC"]
