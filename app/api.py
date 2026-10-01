import logging
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import FileResponse

from app import db
from app.config import setup_logging
from app.models import OrderIn
from app.producer import KafkaPublishError, get_producer, publish_order

setup_logging()
log = logging.getLogger("api")
STATIC = Path(__file__).parent / "static"


@asynccontextmanager
async def lifespan(_app: FastAPI):
    yield
    get_producer().flush(5)  # don't lose buffered messages on shutdown


app = FastAPI(title="OrderStream", version="1.0.0", lifespan=lifespan)


@app.get("/", include_in_schema=False)
def dashboard():
    return FileResponse(STATIC / "index.html")


@app.get("/health")
def health():
    return {"status": "ok"}


@app.post("/orders", status_code=202)
def create_order(order: OrderIn):
    """Publish an order event to Kafka. 202 = accepted; the consumer stores it asynchronously."""
    try:
        return publish_order(order)
    except KafkaPublishError as exc:
        log.error("publish failed: %s", exc)
        raise HTTPException(status_code=503, detail="Message broker unavailable") from exc


@app.get("/orders")
def get_orders(limit: int = Query(50, ge=1, le=500)):
    return db.list_orders(limit)


@app.get("/orders/{order_id}")
def get_order(order_id: str):
    order = db.get_order(order_id)
    if order is None:
        raise HTTPException(status_code=404, detail="Order not found (yet)")
    return order


@app.get("/stats")
def get_stats():
    return db.stats()
