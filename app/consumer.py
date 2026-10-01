"""Kafka consumer: validates order events, stores them in SQLite, dead-letters bad ones.

Delivery semantics: at-least-once. The offset is committed only AFTER the order is
stored; duplicates caused by redelivery are absorbed by the idempotent insert.
If something unexpected fails (e.g. the DB is unavailable) the process exits without
committing, Docker restarts it, and Kafka redelivers from the last committed offset.
"""

import json
import logging
import signal
from datetime import UTC, datetime

from confluent_kafka import Consumer, KafkaError, Producer

from app.config import (
    CONSUMER_GROUP,
    DLQ_TOPIC,
    ORDERS_TOPIC,
    kafka_bootstrap,
    setup_logging,
)
from app.db import save_order
from app.models import OrderEvent

log = logging.getLogger("consumer")
_running = True


def process(raw_value: bytes | str, partition: int, offset: int) -> dict:
    """Validate and store one event. Raises ValueError for malformed/invalid events."""
    event = OrderEvent.model_validate(json.loads(raw_value))  # ValidationError is a ValueError
    data = event.model_dump(mode="json")
    data["status"] = "PROCESSED"
    processed_at = datetime.now(UTC).isoformat()
    is_new = save_order(data, processed_at, partition, offset)
    if not is_new:
        log.info("duplicate %s ignored", data["order_id"])
    return data


def send_to_dlq(dlq: Producer, msg, error: Exception) -> None:
    dlq.produce(
        DLQ_TOPIC,
        key=msg.key(),
        value=msg.value(),
        headers=[("error", str(error)[:500].encode())],
    )
    dlq.flush(5)


def handle_message(msg, dlq: Producer) -> None:
    try:
        order = process(msg.value(), msg.partition(), msg.offset())
        log.info(
            "processed %s (partition %s, offset %s)",
            order["order_id"],
            msg.partition(),
            msg.offset(),
        )
    except ValueError as err:  # includes JSONDecodeError and pydantic ValidationError
        log.warning("rejected message at %s[%s]: %s", msg.partition(), msg.offset(), err)
        send_to_dlq(dlq, msg, err)


def _stop(*_):
    global _running
    _running = False


def main() -> None:
    setup_logging()
    signal.signal(signal.SIGINT, _stop)
    signal.signal(signal.SIGTERM, _stop)

    consumer = Consumer(
        {
            "bootstrap.servers": kafka_bootstrap(),
            "group.id": CONSUMER_GROUP,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,  # we commit manually after storing
        }
    )
    dlq = Producer({"bootstrap.servers": kafka_bootstrap()})
    consumer.subscribe([ORDERS_TOPIC])
    log.info("consumer started (group=%s), waiting for orders...", CONSUMER_GROUP)

    try:
        while _running:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() != KafkaError._PARTITION_EOF:
                    log.error("kafka error: %s", msg.error())
                continue
            handle_message(msg, dlq)
            consumer.commit(message=msg, asynchronous=False)
    finally:
        consumer.close()
        log.info("consumer stopped")


if __name__ == "__main__":
    main()
