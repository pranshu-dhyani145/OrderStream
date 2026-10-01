import json
import logging
import uuid
from datetime import UTC, datetime

from confluent_kafka import KafkaException, Producer

from app.config import DELIVERY_TIMEOUT_S, ORDERS_TOPIC, kafka_bootstrap
from app.models import OrderIn

log = logging.getLogger("producer")
_producer: Producer | None = None


class KafkaPublishError(RuntimeError):
    """Raised when an event could not be confirmed by the broker."""


def get_producer() -> Producer:
    global _producer
    if _producer is None:
        _producer = Producer(
            {
                "bootstrap.servers": kafka_bootstrap(),
                "acks": "all",  # wait for the broker to persist the message
                "enable.idempotence": True,  # no duplicates from producer retries
                "message.timeout.ms": DELIVERY_TIMEOUT_S * 1000,
            }
        )
    return _producer


def build_event(order: OrderIn) -> dict:
    return {
        "order_id": f"ORD-{uuid.uuid4().hex[:8].upper()}",
        **order.model_dump(),
        "status": "CREATED",
        "timestamp": datetime.now(UTC).isoformat(),
    }


def publish_order(order: OrderIn) -> dict:
    """Publish an order event and wait for broker acknowledgement.

    The message key is user_id, so all orders of one user go to the same
    partition and are therefore consumed in the order they were produced.
    """
    event = build_event(order)
    errors: list = []
    delivered: list[tuple[int, int]] = []

    def on_delivery(err, msg):
        if err:
            errors.append(err)
        else:
            delivered.append((msg.partition(), msg.offset()))

    producer = get_producer()
    try:
        producer.produce(
            ORDERS_TOPIC, key=event["user_id"], value=json.dumps(event), callback=on_delivery
        )
    except (BufferError, KafkaException) as exc:
        raise KafkaPublishError(str(exc)) from exc

    remaining = producer.flush(DELIVERY_TIMEOUT_S)
    if remaining or errors or not delivered:
        raise KafkaPublishError(f"delivery not confirmed: {errors or 'timeout'}")

    partition, offset = delivered[0]
    log.info("published %s to partition %s offset %s", event["order_id"], partition, offset)
    return {**event, "kafka": {"partition": partition, "offset": offset}}
