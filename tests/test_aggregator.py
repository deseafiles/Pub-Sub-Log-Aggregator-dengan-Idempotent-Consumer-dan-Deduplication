
import asyncio
import json
import os
import tempfile
import time
import uuid
from datetime import datetime, UTC

import pytest
from fastapi.testclient import TestClient
from httpx import AsyncClient, ASGITransport

try:
    import pytest_asyncio
    asyncio_fixture = pytest_asyncio.fixture
except ImportError:
    asyncio_fixture = pytest.fixture

_tmp_db = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
os.environ["DEDUP_DB_PATH"] = _tmp_db.name

from src.main import app
from src.dedup_store import DedupStore
from src.models import Event  



@pytest.fixture(scope="module")
def client():
    with TestClient(app) as c:
        yield c


@asyncio_fixture
async def async_client():
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as c:
        yield c


@asyncio_fixture
async def fresh_store(tmp_path):
    db_path = str(tmp_path / "test_dedup.db")
    store = DedupStore(db_path=db_path)
    await store.init()
    yield store
    await store.close()


def make_event(topic: str = "test.topic", event_id: str | None = None) -> dict:
    return {
        "topic": topic,
        "event_id": event_id or str(uuid.uuid4()),
        "timestamp": datetime.now(UTC).isoformat(),
        "source": "test-publisher",
        "payload": {"msg": "hello", "val": 42},
    }



class TestSchemaValidation:
    def test_valid_event_model(self):
        e = Event(
            topic="app.logs",
            event_id=str(uuid.uuid4()),
            timestamp=datetime.now(UTC),
            source="svc-a",
            payload={"key": "value"},
        )
        assert e.topic == "app.logs"
        assert e.source == "svc-a"

    def test_topic_no_spaces(self):
        with pytest.raises(Exception):
            Event(topic="topic dengan spasi", source="svc", payload={})

    def test_topic_lowercase(self):
        e = Event(topic="APP.LOGS", source="svc", payload={})
        assert e.topic == "app.logs"

    def test_empty_event_id_rejected(self):
        with pytest.raises(Exception):
            Event(topic="app.logs", event_id="   ", source="svc", payload={})

    def test_publish_empty_list_rejected(self, client):
        resp = client.post("/publish", json={"events": []})
        assert resp.status_code == 400



class TestDedupStore:
    @pytest.mark.asyncio
    async def test_first_event_is_accepted(self, fresh_store):
        result = await fresh_store.mark_processed(
            "t1", "evt-001", "svc", json.dumps({}), datetime.now(UTC).isoformat()
        )
        assert result is True

    @pytest.mark.asyncio
    async def test_duplicate_is_rejected(self, fresh_store):
        kwargs = dict(
            topic="t1",
            event_id="evt-dup",
            source="svc",
            payload=json.dumps({}),
            timestamp=datetime.now(UTC).isoformat(),
        )
        first = await fresh_store.mark_processed(**kwargs)
        second = await fresh_store.mark_processed(**kwargs)
        assert first is True
        assert second is False

    @pytest.mark.asyncio
    async def test_stats_counter_accuracy(self, fresh_store):
        eid = str(uuid.uuid4())
        ts = datetime.now(UTC).isoformat()
        await fresh_store.increment_received(3)
        await fresh_store.mark_processed("t", eid, "s", "{}", ts)
        await fresh_store.mark_processed("t", eid, "s", "{}", ts)  # duplikat

        stats = await fresh_store.get_stats()
        assert stats["received"] == 3
        assert stats["unique_processed"] == 1
        assert stats["duplicate_dropped"] == 1


class TestDedupPersistence:
    @pytest.mark.asyncio
    async def test_dedup_survives_reinit(self, tmp_path):
        db_path = str(tmp_path / "persist.db")
        eid = "persist-event-001"
        ts = datetime.now(UTC).isoformat()

        store1 = DedupStore(db_path=db_path)
        await store1.init()
        result1 = await store1.mark_processed("logs", eid, "svc", "{}", ts)
        await store1.close()

        store2 = DedupStore(db_path=db_path)
        await store2.init()
        result2 = await store2.mark_processed("logs", eid, "svc", "{}", ts)
        await store2.close()

        assert result1 is True  
        assert result2 is False 


class TestAPIEndpoints:
    def test_publish_single_event(self, client):
        resp = client.post("/publish", json={"events": [make_event()]})
        assert resp.status_code == 202
        data = resp.json()
        assert data["enqueued"] == 1

    def test_publish_batch_event(self, client):
        events = [make_event(topic="batch.test") for _ in range(10)]
        resp = client.post("/publish", json={"events": events})
        assert resp.status_code == 202
        assert resp.json()["enqueued"] == 10

    def test_get_events_returns_list(self, client):
        resp = client.get("/events")
        assert resp.status_code == 200
        assert isinstance(resp.json(), list)

    def test_get_stats_structure(self, client):
        resp = client.get("/stats")
        assert resp.status_code == 200
        data = resp.json()
        for field in ("received", "unique_processed", "duplicate_dropped", "topics", "uptime_seconds"):
            assert field in data, f"Field '{field}' tidak ada di /stats"

    def test_health_endpoint(self, client):
        resp = client.get("/health")
        assert resp.status_code == 200
        assert resp.json()["status"] == "ok"


class TestAPIDeduplication:
    def test_duplicate_via_api_dropped_in_stats(self, client):
        eid = f"api-dup-{uuid.uuid4()}"
        event = make_event(topic="dedup.test", event_id=eid)

        stats_before = client.get("/stats").json()
        before_unique = stats_before["unique_processed"]
        before_dup = stats_before["duplicate_dropped"]

        for _ in range(3):
            client.post("/publish", json={"events": [event]})

        time.sleep(0.3) 

        stats_after = client.get("/stats").json()
        delta_unique = stats_after["unique_processed"] - before_unique
        delta_dup = stats_after["duplicate_dropped"] - before_dup

        assert delta_unique == 1, f"Harus tepat 1 unique processed, dapat {delta_unique}"
        assert delta_dup == 2, f"Harus tepat 2 duplicate dropped, dapat {delta_dup}"

    def test_topic_filter_on_get_events(self, client):
        unique_topic = f"filter.test.{uuid.uuid4().hex[:8]}"
        events = [make_event(topic=unique_topic) for _ in range(3)]
        client.post("/publish", json={"events": events})

        time.sleep(0.2)

        resp = client.get(f"/events?topic={unique_topic}")
        assert resp.status_code == 200
        data = resp.json()
        assert all(e["topic"] == unique_topic for e in data), "Ada event topic lain yang masuk"



class TestStress:
    def test_batch_200_events_within_time_limit(self, client):
        events = [make_event(topic="stress.test") for _ in range(200)]
        start = time.perf_counter()
        resp = client.post("/publish", json={"events": events})
        elapsed = time.perf_counter() - start

        assert resp.status_code == 202
        assert elapsed < 5.0, f"Publish 200 event terlalu lambat: {elapsed:.2f}s"
