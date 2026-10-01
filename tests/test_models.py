import pytest
from pydantic import ValidationError

from app.models import OrderIn


def test_valid_order_is_accepted():
    order = OrderIn(user_id="U-101", amount=1499.0, items=["Keyboard", "Mouse"])
    assert order.user_id == "U-101"
    assert order.items == ["Keyboard", "Mouse"]


@pytest.mark.parametrize(
    "payload",
    [
        {"user_id": "U-1", "amount": 0, "items": ["x"]},
        {"user_id": "U-1", "amount": -5, "items": ["x"]},
        {"user_id": "U-1", "amount": 10, "items": []},
        {"user_id": "", "amount": 10, "items": ["x"]},
    ],
)
def test_invalid_orders_are_rejected(payload):
    with pytest.raises(ValidationError):
        OrderIn(**payload)
