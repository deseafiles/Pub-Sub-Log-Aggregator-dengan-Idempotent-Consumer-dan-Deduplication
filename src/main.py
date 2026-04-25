import asyncio
import logging
import time
from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import JSONResponse
from .consumer import EventConsumer
from .dedup_store import DedupStore
from .models import Event, EventResponse, PublishRequest, StatsResponse

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
logger = logging.getLogger(__name__)

_store: DedupStore | None = None
_queue: asyncio.Queue | None = None
_consumer: EventConsumer | None = None
_start_time: float = 0.0


@asynccontextmanager
async def lifespan(app: FastAPI):
    global _store, _queue, _consumer, _start_time

    _start_time = time.time()
    _queue = asyncio.Queue(maxsize=0)
    _store = DedupStore()
    await _store.init()

    _consumer = EventConsumer(queue=_queue, store=_store)
    await _consumer.start()

    logger.info("Aggregator service started. Ready to receive events.")
    yield

    await _consumer.stop()
    await _store.close()
    logger.info("Aggregator service shut down cleanly.")


app = FastAPI(
    title="Pub-Sub Log Aggregator",
    description=(
        "Layanan aggregator log berbasis Pub-Sub dengan idempotent consumer "
        "dan persistent deduplication (SQLite). "
        "Mendukung at-least-once delivery dengan jaminan exactly-once processing."
    ),
    version="1.0.0",
    lifespan=lifespan,
)


def _get_components() -> tuple[asyncio.Queue, DedupStore]:
    if _queue is None or _store is None:
        raise HTTPException(status_code=503, detail="Service not ready.")
    return _queue, _store



@app.post(
    "/publish",
    summary="Publish event(s) ke aggregator",
    response_description="Jumlah event yang diterima dan di-enqueue",
    status_code=202,
)
async def publish(body: PublishRequest):
    queue, store = _get_components()

    if not body.events:
        raise HTTPException(status_code=400, detail="Daftar events tidak boleh kosong.")

    count = len(body.events)
    for event in body.events:
        await queue.put(event)

    await store.increment_received(count)

    logger.info("RECEIVED: %d event(s) enqueued.", count)
    return {
        "status": "accepted",
        "enqueued": count,
        "message": f"{count} event(s) diterima dan sedang diproses.",
    }


@app.get(
    "/events",
    response_model=list[EventResponse],
    summary="Ambil daftar event unik yang sudah diproses",
)
async def get_events(
    topic: str | None = Query(None, description="Filter berdasarkan nama topic"),
):
    _, store = _get_components()
    await asyncio.sleep(0.05)
    events = await store.get_events(topic=topic)
    return events


@app.get(
    "/stats",
    response_model=StatsResponse,
    summary="Statistik sistem aggregator",
)
async def get_stats():
    _, store = _get_components()
    raw = await store.get_stats()
    topics = await store.get_topics()
    uptime = time.time() - _start_time

    return StatsResponse(
        received=raw.get("received", 0),
        unique_processed=raw.get("unique_processed", 0),
        duplicate_dropped=raw.get("duplicate_dropped", 0),
        topics=topics,
        uptime_seconds=round(uptime, 2),
    )


@app.get("/health", summary="Health check", include_in_schema=False)
async def health():
    return {"status": "ok", "uptime_seconds": round(time.time() - _start_time, 2)}
