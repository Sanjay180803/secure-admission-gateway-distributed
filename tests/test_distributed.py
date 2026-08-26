"""
Run: python3 tests/test_distributed.py
Requires a running Redis on localhost:6379.
"""
import os
import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import concurrent.futures
from shared.ledger import DistributedLedger

l = DistributedLedger()

print("--- Test 1: reservation respects budget ---")
l.reset()
nonces = l.reserve_credits(5, 5.0)
assert len(nonces) == 5
nonces2 = l.reserve_credits(10, 5.0)
assert len(nonces2) == 5, f"expected 5 (budget=10, 5 already outstanding), got {len(nonces2)}"
print("PASS: budget correctly capped issuance")

print("\n--- Test 2: spend is exactly-once, cluster-wide ---")
l.reset()
nonces = l.reserve_credits(1, 5.0)
n = nonces[0]
assert l.spend_credit(n) is True
assert l.spend_credit(n) is False, "replay was allowed!"
print("PASS: spend is exactly-once")

print("\n--- Test 3: spending frees the reserved slot ---")
l.reset()
nonces = l.reserve_credits(10, 5.0)
assert len(nonces) == 10
l.spend_credit(nonces[0])
assert l.outstanding_count() == 9
more = l.reserve_credits(5, 5.0)
assert len(more) == 1, f"expected exactly 1 freed slot, got {len(more)}"
print("PASS: spending a credit correctly frees capacity for a new one")

print("\n--- Test 4: no over-issuance under 20-way concurrent reservation ---")
l.reset()
def try_reserve():
    return DistributedLedger().reserve_credits(3, 5.0)
with concurrent.futures.ThreadPoolExecutor(max_workers=20) as ex:
    results = list(ex.map(lambda _: try_reserve(), range(20)))
total = sum(len(r) for r in results)
assert total <= 10, f"OVER-ISSUANCE: {total} issued against budget of 10"
print(f"PASS: {total} issued (budget=10) despite 20 concurrent racing attempts")

print("\n--- Test 5: trust/suspicion blocking works through the ledger ---")
l.reset()
for _ in range(5):
    l.record_violation("bad-client", "bad_signature")
assert l.is_blocked("bad-client") is True
print("PASS: repeated violations correctly block a client via the shared ledger")

print("\nALL DISTRIBUTED LEDGER TESTS PASSED")
