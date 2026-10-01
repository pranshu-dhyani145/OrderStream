import pytest
from fastapi.testclient import TestClient

from app import api, db, producer
from tests.conftest import FakeProducer


@pytest.fixture
def client(monkeypatch):
    monkeypatch.setattr(producer, "get_producer", lambda: FakeProducer())
    monkeypatch.setattr(api, "get_producer", lambda: FakeProducer())
    with TestClient(api.app) as c:
        yield c


PAYLOAD = {"user_id": "U-101", "amount": 1499, "items": ["Keyboard", "Mouse"]}


def test_health(client):
    r = client.get("/health")
    assert r.status_code == 200
    assert r.json() == {"status": "ok"}


def test_post_order_returns_202_with_kafka_metadata(client):
    r = client.post("/orders", json=PAYLOAD)
    assert r.status_code == 202
    body = r.json()
    assert body["order_id"].startswith("ORD-")
    assert "partition" in body["kafka"]


def test_post_invalid_order_returns_422(client):
    r = client.post("/orders", json={**PAYLOAD, "amount": -10})
    assert r.status_code == 422


def test_post_returns_503_when_kafka_is_down(client, monkeypatch):
    monkeypatch.setattr(producer, "get_producer", lambda: FakeProducer(fail=True))
    r = client.post("/orders", json=PAYLOAD)
    assert r.status_code == 503


def test_get_orders_and_404(client):
    db.save_order(
        {
            "order_id": "ORD-9",
            "user_id": "U-1",
            "amount": 10.0,
            "items": ["x"],
            "status": "PROCESSED",
            "timestamp": "2026-09-30T10:00:00+00:00",
        },
        "2026-09-30T10:00:01+00:00",
        0,
        0,
    )
    assert [o["order_id"] for o in client.get("/orders").json()] == ["ORD-9"]
    assert client.get("/orders/ORD-9").status_code == 200
    assert client.get("/orders/nope").status_code == 404
    assert client.get("/stats").json()["total_orders"] == 1
