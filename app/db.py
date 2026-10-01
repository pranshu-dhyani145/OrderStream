"""SQLite storage shared by the API (reads) and the consumer (writes) via a Docker volume."""

import json
import sqlite3
from contextlib import contextmanager

from app.config import db_path

SCHEMA = """
CREATE TABLE IF NOT EXISTS orders (
    order_id        TEXT PRIMARY KEY,
    user_id         TEXT NOT NULL,
    amount          REAL NOT NULL,
    items           TEXT NOT NULL,
    status          TEXT NOT NULL,
    created_at      TEXT NOT NULL,
    processed_at    TEXT NOT NULL,
    kafka_partition INTEGER,
    kafka_offset    INTEGER
)
"""


@contextmanager
def connect():
    conn = sqlite3.connect(db_path(), timeout=5)
    conn.row_factory = sqlite3.Row
    try:
        conn.execute("PRAGMA journal_mode=WAL")  # readers don't block the writer
        conn.execute("PRAGMA busy_timeout=5000")
        conn.execute(SCHEMA)
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def save_order(event: dict, processed_at: str, partition: int, offset: int) -> bool:
    """Insert an order. Returns False if it already existed (duplicate delivery)."""
    with connect() as conn:
        # INSERT OR IGNORE makes processing idempotent under at-least-once delivery.
        cur = conn.execute(
            "INSERT OR IGNORE INTO orders VALUES (?,?,?,?,?,?,?,?,?)",
            (
                event["order_id"],
                event["user_id"],
                event["amount"],
                json.dumps(event["items"]),
                event["status"],
                event["timestamp"],
                processed_at,
                partition,
                offset,
            ),
        )
        return cur.rowcount == 1


def _row(r: sqlite3.Row) -> dict:
    d = dict(r)
    d["items"] = json.loads(d["items"])
    return d


def list_orders(limit: int = 50) -> list[dict]:
    with connect() as conn:
        rows = conn.execute(
            "SELECT * FROM orders ORDER BY processed_at DESC LIMIT ?", (limit,)
        ).fetchall()
        return [_row(r) for r in rows]


def get_order(order_id: str) -> dict | None:
    with connect() as conn:
        r = conn.execute("SELECT * FROM orders WHERE order_id = ?", (order_id,)).fetchone()
        return _row(r) if r else None


def stats() -> dict:
    with connect() as conn:
        total, revenue = conn.execute(
            "SELECT COUNT(*), COALESCE(SUM(amount), 0) FROM orders"
        ).fetchone()
        by_partition = {
            str(r["kafka_partition"]): r["c"]
            for r in conn.execute(
                "SELECT kafka_partition, COUNT(*) AS c FROM orders GROUP BY kafka_partition"
            )
        }
        return {"total_orders": total, "total_revenue": revenue, "by_partition": by_partition}
