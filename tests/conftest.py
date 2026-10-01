import pytest


@pytest.fixture(autouse=True)
def temp_db(tmp_path, monkeypatch):
    """Every test gets its own empty SQLite file."""
    monkeypatch.setenv("DB_PATH", str(tmp_path / "test.db"))


class FakeMsg:
    """Minimal stand-in for a confluent_kafka.Message."""

    def __init__(self, value, partition=0, offset=0, key=b"U-1"):
        self._value, self._partition, self._offset, self._key = value, partition, offset, key

    def value(self):
        return self._value

    def partition(self):
        return self._partition

    def offset(self):
        return self._offset

    def key(self):
        return self._key


class FakeProducer:
    """Records produced messages and confirms delivery immediately (no broker needed)."""

    def __init__(self, fail=False):
        self.fail = fail
        self.sent = []

    def produce(self, topic, key=None, value=None, callback=None, headers=None):
        self.sent.append({"topic": topic, "key": key, "value": value, "headers": headers})
        if callback:
            if self.fail:
                callback("broker down", None)
            else:
                callback(None, FakeMsg(value, partition=1, offset=len(self.sent) - 1))

    def flush(self, timeout=None):
        return 0
