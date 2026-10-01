"""Central configuration. Values are read lazily so tests can override env vars."""

import logging
import os

ORDERS_TOPIC = "orders"
DLQ_TOPIC = "orders.dlq"
CONSUMER_GROUP = "order-processors"
DELIVERY_TIMEOUT_S = 5


def kafka_bootstrap() -> str:
    return os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")


def db_path() -> str:
    return os.getenv("DB_PATH", "orders.db")


def setup_logging() -> None:
    logging.basicConfig(
        level=os.getenv("LOG_LEVEL", "INFO"),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
