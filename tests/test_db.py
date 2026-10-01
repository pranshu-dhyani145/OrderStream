from app import db


def event(order_id, user="U-1", amount=100.0):
    return {
        "order_id": order_id,
        "user_id": user,
        "amount": amount,
        "items": ["x"],
        "status": "PROCESSED",
        "timestamp": "2026-09-30T10:00:00+00:00",
    }


def test_save_and_fetch_order():
    assert db.save_order(event("ORD-1"), "2026-09-30T10:00:01+00:00", 0, 0) is True
    assert db.get_order("ORD-1")["amount"] == 100.0
    assert db.get_order("missing") is None


def test_stats_aggregate_by_partition():
    db.save_order(event("ORD-1", amount=100), "2026-09-30T10:00:01+00:00", 0, 0)
    db.save_order(event("ORD-2", amount=50), "2026-09-30T10:00:02+00:00", 1, 0)
    db.save_order(event("ORD-3", amount=25), "2026-09-30T10:00:03+00:00", 1, 1)

    s = db.stats()
    assert s["total_orders"] == 3
    assert s["total_revenue"] == 175
    assert s["by_partition"] == {"0": 1, "1": 2}
