import json

import pytest

from app import producer
from app.models import OrderIn
from tests.conftest import FakeProducer


def order():
    return OrderIn(user_id="U-7", amount=250.0, items=["Book"])


def test_publish_sends_event_keyed_by_user_id(monkeypatch):
    fake = FakeProducer()
    monkeypatch.setattr(producer, "get_producer", lambda: fake)

    result = producer.publish_order(order())

    assert len(fake.sent) == 1
    sent = fake.sent[0]
    assert sent["topic"] == "orders"
    assert sent["key"] == "U-7"  # same user -> same partition -> ordered
    body = json.loads(sent["value"])
    assert body["order_id"].startswith("ORD-")
    assert body["status"] == "CREATED"
    assert result["kafka"] == {"partition": 1, "offset": 0}


def test_publish_raises_when_broker_rejects(monkeypatch):
    monkeypatch.setattr(producer, "get_producer", lambda: FakeProducer(fail=True))
    with pytest.raises(producer.KafkaPublishError):
        producer.publish_order(order())
