"""
Unit and concurrency tests for the Redis-backed ledger.

Run: pytest tests/        (requires Redis on localhost:6379, or REDIS_URL)
"""
import concurrent.futures

import pytest

from conftest import REDIS_URL, requires_redis
from shared.ledger import MAX_BUDGET, DistributedLedger

pytestmark = requires_redis


def test_reservation_respects_budget(ledger):
    assert len(ledger.reserve_credits(5, 5.0)) == 5
    # budget is 10 and 5 are already outstanding
    assert len(ledger.reserve_credits(10, 5.0)) == 5


def test_spend_succeeds_once_then_rejects_replay(ledger):
    (nonce,) = ledger.reserve_credits(1, 5.0)
    assert ledger.spend_credit(nonce) is True
    assert ledger.spend_credit(nonce) is False


def test_spending_frees_the_reserved_slot(ledger):
    nonces = ledger.reserve_credits(MAX_BUDGET, 5.0)
    assert len(nonces) == MAX_BUDGET
    ledger.spend_credit(nonces[0])
    assert ledger.outstanding_count() == MAX_BUDGET - 1
    assert len(ledger.reserve_credits(5, 5.0)) == 1


def test_expired_reservations_release_capacity(ledger):
    assert len(ledger.reserve_credits(MAX_BUDGET, 0.05)) == MAX_BUDGET
    import time

    time.sleep(0.1)
    assert len(ledger.reserve_credits(3, 5.0)) == 3


@pytest.mark.parametrize("workers", [20, 50])
def test_no_over_issuance_under_concurrent_reservation(ledger, workers):
    def reserve(_):
        return DistributedLedger(REDIS_URL).reserve_credits(3, 5.0)

    with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as ex:
        results = list(ex.map(reserve, range(workers)))

    assert sum(len(r) for r in results) == MAX_BUDGET


def test_concurrent_replay_has_exactly_one_winner(ledger):
    (nonce,) = ledger.reserve_credits(1, 5.0)

    def spend(_):
        return DistributedLedger(REDIS_URL).spend_credit(nonce)

    with concurrent.futures.ThreadPoolExecutor(max_workers=20) as ex:
        outcomes = list(ex.map(spend, range(20)))

    assert outcomes.count(True) == 1
    assert outcomes.count(False) == 19


def test_repeated_violations_block_client(ledger):
    for _ in range(3):
        ledger.record_violation("bad-client", "bad_signature")
    assert ledger.is_blocked("bad-client") is True
    assert ledger.is_blocked("other-client") is False
