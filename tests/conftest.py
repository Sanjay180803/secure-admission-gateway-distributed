import os
import sys

import pytest
import redis

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

REDIS_URL = os.environ.get("REDIS_URL", "redis://localhost:6379/0")


def _redis_available() -> bool:
    try:
        return redis.from_url(REDIS_URL).ping()
    except redis.exceptions.ConnectionError:
        return False


requires_redis = pytest.mark.skipif(not _redis_available(), reason=f"no Redis at {REDIS_URL}")


@pytest.fixture
def ledger():
    from shared.ledger import DistributedLedger

    led = DistributedLedger(REDIS_URL)
    led.reset()
    yield led
    led.reset()
