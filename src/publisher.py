import argparse
import asyncio
import logging
import random
import time
import uuid
from datetime import datetime

import httpx

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("publisher")

TOPICS = ["app.logs", "auth.events", "payment.events", "user.activity", "system.metrics"]
SOURCES = ["service-a", "service-b", "gateway", "worker-1", "worker-2"]
BATCH_SIZE = 50


def generate_event(topic: str | None = None) -> dict:
    return {
        "topic": topic or random.choice(TOPICS),
        "event_id": str(uuid.uuid4()),
        "timestamp": datetime.utcnow().isoformat(),
        "source": random.choice(SOURCES),
        "payload": {
            "level": random.choice(["INFO", "WARN", "ERROR"]),
            "message": f"Sample log message #{random.randint(1, 9999)}",
            "value": round(random.uniform(0.0, 100.0), 3),
        },
    }


async def publish_batch(client: httpx.AsyncClient, host: str, events: list[dict]) -> bool:
    try:
        resp = await client.post(
            f"{host}/publish",
            json={"events": events},
            timeout=30.0,
        )
        resp.raise_for_status()
        return True
    except Exception as e:
        logger.error("Publish failed: %s", e)
        return False


async def run(host: str, total: int, dup_rate: float) -> None:
    logger.info(
        "Publisher starting — target=%s, total=%d events, dup_rate=%.0f%%",
        host, total, dup_rate * 100
    )

    unique_count = int(total * (1 - dup_rate))
    dup_count = total - unique_count

    unique_events = [generate_event() for _ in range(unique_count)]

    dup_events = [random.choice(unique_events).copy() for _ in range(dup_count)]
    for e in dup_events:
        e["timestamp"] = datetime.utcnow().isoformat()

    all_events = unique_events + dup_events
    random.shuffle(all_events)

    start = time.perf_counter()
    sent = 0
    async with httpx.AsyncClient() as client:
        for i in range(0, len(all_events), BATCH_SIZE):
            batch = all_events[i : i + BATCH_SIZE]
            ok = await publish_batch(client, host, batch)
            if ok:
                sent += len(batch)
            if sent % 500 == 0 and sent > 0:
                elapsed = time.perf_counter() - start
                logger.info("Progress: %d/%d events sent (%.1fs elapsed)", sent, total, elapsed)

    elapsed = time.perf_counter() - start
    logger.info(
        "Publisher done — sent=%d (unique=%d, dup=%d) in %.2fs (%.0f evt/s)",
        sent, unique_count, dup_count, elapsed, sent / elapsed if elapsed > 0 else 0,
    )


def main():
    parser = argparse.ArgumentParser(description="Pub-Sub Log Aggregator Publisher")
    parser.add_argument("--host", default="http://localhost:8080")
    parser.add_argument("--total", type=int, default=5000)
    parser.add_argument("--dup-rate", type=float, default=0.2)
    args = parser.parse_args()
    asyncio.run(run(args.host, args.total, args.dup_rate))


if __name__ == "__main__":
    main()
