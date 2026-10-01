from datetime import datetime

from pydantic import BaseModel, Field


class OrderIn(BaseModel):
    """What a client sends to POST /orders."""

    user_id: str = Field(min_length=1, max_length=64, examples=["U-101"])
    amount: float = Field(gt=0, examples=[1499.0])
    items: list[str] = Field(min_length=1, examples=[["Keyboard", "Mouse"]])


class OrderEvent(BaseModel):
    """The event that travels through Kafka (producer output = consumer input)."""

    order_id: str
    user_id: str = Field(min_length=1)
    amount: float = Field(gt=0)
    items: list[str] = Field(min_length=1)
    status: str
    timestamp: datetime
