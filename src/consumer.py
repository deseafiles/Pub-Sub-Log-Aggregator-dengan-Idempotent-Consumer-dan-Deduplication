import asyncio
import json
import logging
from datetime import datetime

from .dedup_store import DedupStore
from .models import Event

logger = logging.getLogger(__name__)


class EventConsumer:
    def __init__(self, queue: asyncio.Queue, store: DedupStore):
        self._queue = queue
        self._store = store
        self._running = False
        self._task: asyncio.Task | None = None

    async def start(self) -> None:
        self._running = True
        self._task = asyncio.create_task(self._consume_loop(), name="event-consumer")
        logger.info("EventConsumer started.")

    async def stop(self) -> None:
        self._running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        logger.info("EventConsumer stopped.")

    async def _consume_loop(self) -> None:
        while self._running:
            try:
                event: Event = await asyncio.wait_for(self._queue.get(), timeout=1.0)
                await self._process(event)
                self._queue.task_done()
            except asyncio.TimeoutError:
                continue
            except asyncio.CancelledError:
                break
            except Exception as exc:
                logger.error("Consumer error: %s", exc, exc_info=True)

    async def _process(self, event: Event) -> None:
        topic = event.topic
        event_id = event.event_id
        timestamp = (
            event.timestamp.isoformat()
            if isinstance(event.timestamp, datetime)
            else str(event.timestamp)
        )

        is_new = await self._store.mark_processed(
            topic=topic,
            event_id=event_id,
            source=event.source,
            payload=json.dumps(event.payload),
            timestamp=timestamp,
        )

        if is_new:
            logger.info(
                "PROCESSED: topic=%s event_id=%s source=%s",
                topic, event_id, event.source,
            )
        else:
            logger.warning(
                "DUPLICATE DROPPED: topic=%s event_id=%s — already processed",
                topic, event_id,
            )
