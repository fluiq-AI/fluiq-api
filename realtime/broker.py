"""In-process pub/sub for SSE trace streaming.

A single :class:`TraceBroker` instance is shared by the API process. The
Kafka consumer (see :mod:`realtime.consumer`) calls :meth:`publish` for
every persisted-trace notification, and each open SSE connection holds
its own subscriber queue obtained via :meth:`subscribe`.

Per-subscriber queues are bounded; on overflow the oldest pending item
is dropped so a slow client cannot grow memory without bound. Replicas
do not coordinate — every API process runs its own broker and consumer
with a unique Kafka group_id, so each replica receives every event and
fans out only to *its* connected clients.
"""
import asyncio
import logging
from collections import defaultdict
from contextlib import asynccontextmanager
from typing import AsyncIterator, Optional

logger = logging.getLogger(__name__)

DEFAULT_QUEUE_MAXSIZE = 256


class TraceBroker:
    def __init__(self, queue_maxsize: int = DEFAULT_QUEUE_MAXSIZE) -> None:
        self._queue_maxsize = queue_maxsize
        # org_id -> set of subscriber queues. We use a regular set rather
        # than a WeakSet because each queue is owned by an active SSE
        # connection and lives until that connection ends.
        self._subscribers: dict[str, set[asyncio.Queue]] = defaultdict(set)
        self._lock = asyncio.Lock()

    async def publish(self, organization_id: str, message: dict) -> None:
        """Deliver `message` to every subscriber for `organization_id`.

        Slow consumers drop their oldest pending item rather than block
        the producer (the Kafka consumer loop). This keeps end-to-end
        latency bounded under bursty trace volume.
        """
        if not organization_id:
            return
        async with self._lock:
            queues = list(self._subscribers.get(organization_id, ()))
        for q in queues:
            try:
                q.put_nowait(message)
            except asyncio.QueueFull:
                try:
                    q.get_nowait()
                except asyncio.QueueEmpty:
                    pass
                try:
                    q.put_nowait(message)
                except asyncio.QueueFull:
                    logger.warning(
                        "[SSE] Subscriber queue still full after drop org=%s",
                        organization_id,
                    )

    async def _add(self, organization_id: str, q: asyncio.Queue) -> None:
        async with self._lock:
            self._subscribers[organization_id].add(q)

    async def _remove(self, organization_id: str, q: asyncio.Queue) -> None:
        async with self._lock:
            subs = self._subscribers.get(organization_id)
            if subs is None:
                return
            subs.discard(q)
            if not subs:
                self._subscribers.pop(organization_id, None)

    @asynccontextmanager
    async def subscribe(
        self,
        organization_id: str,
    ) -> AsyncIterator[asyncio.Queue]:
        """Yield a per-subscriber :class:`asyncio.Queue` for `organization_id`.

        Returning the queue directly (rather than an async generator over
        it) lets callers wrap ``queue.get()`` in :func:`asyncio.wait_for`
        for heartbeats without the generator-cancellation pitfall: when
        ``wait_for`` cancels ``__anext__`` of an async generator, the
        generator's frame is torn down and subsequent ``__anext__`` calls
        raise :class:`StopAsyncIteration`, ending the stream after the
        first idle timeout. With a plain queue the cancellation cancels
        only the current ``get()`` coroutine and the queue itself remains
        usable. Per-message API-key filtering lives in the caller.
        """
        q: asyncio.Queue = asyncio.Queue(maxsize=self._queue_maxsize)
        await self._add(organization_id, q)
        try:
            yield q
        finally:
            await self._remove(organization_id, q)

    def subscriber_count(self, organization_id: Optional[str] = None) -> int:
        if organization_id is None:
            return sum(len(s) for s in self._subscribers.values())
        return len(self._subscribers.get(organization_id, ()))


trace_broker = TraceBroker()

__all__ = ["TraceBroker", "trace_broker", "DEFAULT_QUEUE_MAXSIZE"]
