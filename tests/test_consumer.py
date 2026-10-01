import json

import pytest

from app import consumer, db
from tests.conftest import FakeMsg, FakeProducer


def valid_event(order_id="ORD-AAAA1111"):
    return {
        "order_id": order_id,
        "user_id": "U-1",
        "amount": 99.5,
        "items": ["Pen"],
        "status": "CREATED",
        "timestamp": "2026-09-30T10:00:00+00:00",
    }


def test_valid_event_is_processed_and_stored():
    consumer.process(json.dumps(valid_event()), partition=1, offset=5)

    stored = db.get_order("ORD-AAAA1111")
    assert stored["status"] == "PROCESSED"
    assert stored["kafka_partition"] == 1
    assert stored["items"] == ["Pen"]


def test_invalid_event_raises_value_error():
    bad = valid_event()
    bad["amount"] = -1
    with pytest.raises(ValueError):
        consumer.process(json.dumps(bad), partition=0, offset=0)
    assert db.list_orders() == []


def test_duplicate_delivery_is_idempotent():
    raw = json.dumps(valid_event())
    consumer.process(raw, 0, 0)
    consumer.process(raw, 0, 0)  # Kafka redelivered the same message
    assert len(db.list_orders()) == 1


def test_bad_message_goes_to_dead_letter_topic():
    dlq = FakeProducer()
    consumer.handle_message(FakeMsg(b"not json at all"), dlq)

    assert len(dlq.sent) == 1
    assert dlq.sent[0]["topic"] == "orders.dlq"
    assert dlq.sent[0]["value"] == b"not json at all"
    assert db.list_orders() == []
