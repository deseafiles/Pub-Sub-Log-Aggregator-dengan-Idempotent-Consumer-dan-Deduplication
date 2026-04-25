import asyncio
import logging
import os
from datetime import datetime, UTC

import aiosqlite

logger = logging.getLogger(__name__)

DB_PATH = os.environ.get("DEDUP_DB_PATH", "/app/data/dedup.db")


class DedupStore:
    def __init__(self, db_path: str = DB_PATH):
        self.db_path = db_path
        self._lock = asyncio.Lock()
        self._db: aiosqlite.Connection | None = None

    async def init(self) -> None:
        os.makedirs(os.path.dirname(self.db_path), exist_ok=True)
        self._db = await aiosqlite.connect(self.db_path)
        await self._db.execute("PRAGMA journal_mode=WAL;")
        await self._db.execute("""
            CREATE TABLE IF NOT EXISTS processed_events (
                topic        TEXT NOT NULL,
                event_id     TEXT NOT NULL,
                source       TEXT,
                payload      TEXT,
                timestamp    TEXT,
                processed_at TEXT NOT NULL,
                PRIMARY KEY (topic, event_id)
            )
        """)
        await self._db.execute("""
            CREATE TABLE IF NOT EXISTS stats (
                key   TEXT PRIMARY KEY,
                value INTEGER NOT NULL DEFAULT 0
            )
        """)
        for key in ("received", "unique_processed", "duplicate_dropped"):
            await self._db.execute(
                "INSERT OR IGNORE INTO stats (key, value) VALUES (?, 0)", (key,)
            )
        await self._db.commit()
        logger.info("DedupStore initialized at %s", self.db_path)

    async def close(self) -> None:
        if self._db:
            await self._db.close()

    async def is_duplicate(self, topic: str, event_id: str) -> bool:
        async with self._lock:
            async with self._db.execute(
                "SELECT 1 FROM processed_events WHERE topic=? AND event_id=?",
                (topic, event_id),
            ) as cursor:
                row = await cursor.fetchone()
            return row is not None

    async def mark_processed(
        self,
        topic: str,
        event_id: str,
        source: str,
        payload: str,
        timestamp: str,
    ) -> bool:
        processed_at = datetime.now(UTC).isoformat()
        async with self._lock:
            try:
                await self._db.execute(
                    """
                    INSERT INTO processed_events
                        (topic, event_id, source, payload, timestamp, processed_at)
                    VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    (topic, event_id, source, payload, timestamp, processed_at),
                )
                await self._db.execute(
                    "UPDATE stats SET value = value + 1 WHERE key = 'unique_processed'"
                )
                await self._db.commit()
                return True
            except aiosqlite.IntegrityError:
                logger.warning(
                    "DUPLICATE DETECTED: topic=%s event_id=%s — skipped", topic, event_id
                )
                await self._db.execute(
                    "UPDATE stats SET value = value + 1 WHERE key = 'duplicate_dropped'"
                )
                await self._db.commit()
                return False

    async def increment_received(self, count: int = 1) -> None:
        async with self._lock:
            await self._db.execute(
                "UPDATE stats SET value = value + ? WHERE key = 'received'", (count,)
            )
            await self._db.commit()

    async def get_stats(self) -> dict[str, int]:
        async with self._lock:
            async with self._db.execute("SELECT key, value FROM stats") as cursor:
                rows = await cursor.fetchall()
        return {row[0]: row[1] for row in rows}

    async def get_events(self, topic: str | None = None) -> list[dict]:
        query = "SELECT topic, event_id, source, payload, timestamp, processed_at FROM processed_events"
        params: tuple = ()
        if topic:
            query += " WHERE topic = ?"
            params = (topic,)
        query += " ORDER BY processed_at DESC"
        async with self._lock:
            async with self._db.execute(query, params) as cursor:
                rows = await cursor.fetchall()
        import json
        return [
            {
                "topic": r[0],
                "event_id": r[1],
                "source": r[2],
                "payload": json.loads(r[3]) if r[3] else {},
                "timestamp": r[4],
                "processed_at": r[5],
            }
            for r in rows
        ]

    async def get_topics(self) -> list[str]:
        async with self._lock:
            async with self._db.execute(
                "SELECT DISTINCT topic FROM processed_events ORDER BY topic"
            ) as cursor:
                rows = await cursor.fetchall()
        return [r[0] for r in rows]
