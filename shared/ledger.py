"""
Coordination substrate shared by every gateway replica.

The problem this solves: if each replica tracked "outstanding credits" and
"current budget" purely in its own memory (like the single-instance
version of this project), then N replicas behind a load balancer would
each independently think they have headroom -- and collectively admit up
to N times the intended system-wide capacity. That's a real, well-known
failure mode of naively "just add more instances" scaling for anything
with a global limit (rate limiters, inventory counts, seat reservations).

The fix: push the shared, contended state (outstanding-credit count,
adaptive budget, spent-nonce set, per-client suspicion) into Redis, and
make the operations that must be exact -- "reserve N credit slots without
over-shooting the budget," "mark a nonce spent exactly once" -- atomic via
Redis Lua scripts / SETNX, so concurrent replicas can't race each other
into an inconsistent state.
"""
import time
import uuid

import redis

OUTSTANDING_ZSET = "gateway:outstanding"     # nonce -> expires_at (sorted set)
EWMA_DELAY_KEY = "gateway:ewma_delay_ms"
BUDGET_KEY = "gateway:budget"
TRUST_HASH_PREFIX = "gateway:trust:"          # + client_id -> {suspicion, violations}

TARGET_DELAY_MS = 20.0
MIN_BUDGET = 1
MAX_BUDGET = 10
EWMA_ALPHA = 0.3

SUSPICION_PENALTY = {
    "bad_signature": 0.40,
    "replayed": 0.35,
    "malformed_token": 0.30,
    "expired": 0.05,
}
BLOCK_THRESHOLD = 0.8
SUSPICION_DECAY_PER_SEC = 0.01

# Atomically: purge expired reservations, then reserve as many of the
# requested nonces as the remaining budget allows.
_RESERVE_SCRIPT = """
local outstanding_key = KEYS[1]
local budget_key = KEYS[2]
local now = tonumber(ARGV[1])
local requested = tonumber(ARGV[2])
local ttl = tonumber(ARGV[3])
local default_budget = tonumber(ARGV[4])

redis.call('ZREMRANGEBYSCORE', outstanding_key, '-inf', now)
local current = redis.call('ZCARD', outstanding_key)
local budget = tonumber(redis.call('GET', budget_key) or default_budget)
local available = budget - current
if available < 0 then available = 0 end
local to_issue = math.min(available, requested)

local issued = {}
local expires_at = now + ttl
for i = 1, to_issue do
    local nonce = ARGV[4 + i]
    redis.call('ZADD', outstanding_key, expires_at, nonce)
    table.insert(issued, nonce)
end
return issued
"""

# Atomically update the shared EWMA delay estimate and recompute the
# shared budget from it (additive-increase / multiplicative-decrease).
_UPDATE_DELAY_SCRIPT = """
local ewma_key = KEYS[1]
local budget_key = KEYS[2]
local sample = tonumber(ARGV[1])
local alpha = tonumber(ARGV[2])
local target = tonumber(ARGV[3])
local min_budget = tonumber(ARGV[4])
local max_budget = tonumber(ARGV[5])

local old_ewma = tonumber(redis.call('GET', ewma_key) or '0')
local new_ewma = alpha * sample + (1 - alpha) * old_ewma
redis.call('SET', ewma_key, new_ewma)

local budget = tonumber(redis.call('GET', budget_key) or max_budget)
if new_ewma > target then
    budget = math.max(min_budget, math.floor(budget * 0.7))
else
    budget = math.min(max_budget, budget + 2)
end
redis.call('SET', budget_key, budget)
return {tostring(new_ewma), tostring(budget)}
"""


class DistributedLedger:
    def __init__(self, redis_url: str = "redis://localhost:6379/0"):
        self.r = redis.from_url(redis_url, decode_responses=True)
        self._reserve = self.r.register_script(_RESERVE_SCRIPT)
        self._update_delay = self.r.register_script(_UPDATE_DELAY_SCRIPT)

    # --- credit reservation (the core cross-replica safety property) -----
    def reserve_credits(self, requested: int, ttl_seconds: float) -> list[str]:
        now = time.time()
        candidate_nonces = [uuid.uuid4().hex for _ in range(requested)]
        issued = self._reserve(
            keys=[OUTSTANDING_ZSET, BUDGET_KEY],
            args=[now, requested, ttl_seconds, MAX_BUDGET, *candidate_nonces],
        )
        return issued

    def spend_credit(self, nonce: str) -> bool:
        """
        Marks a nonce as spent exactly once, cluster-wide. SETNX is atomic
        across all replicas talking to the same Redis, which is exactly
        the "only one of you gets to spend this" guarantee we need.
        Also frees the reserved slot so the budget reflects real usage.
        """
        first_time = self.r.set(f"gateway:spent:{nonce}", "1", nx=True, ex=60)
        self.r.zrem(OUTSTANDING_ZSET, nonce)
        return bool(first_time)

    def is_already_spent(self, nonce: str) -> bool:
        return self.r.exists(f"gateway:spent:{nonce}") == 1

    # --- delay-adaptive shared budget --------------------------------------
    def record_delay_and_get_state(self, delay_ms: float) -> tuple[float, int]:
        ewma_str, budget_str = self._update_delay(
            keys=[EWMA_DELAY_KEY, BUDGET_KEY],
            args=[delay_ms, EWMA_ALPHA, TARGET_DELAY_MS, MIN_BUDGET, MAX_BUDGET],
        )
        return float(ewma_str), int(float(budget_str))

    def current_state(self) -> tuple[float, int]:
        ewma = float(self.r.get(EWMA_DELAY_KEY) or 0.0)
        budget = int(float(self.r.get(BUDGET_KEY) or MAX_BUDGET))
        return ewma, budget

    def outstanding_count(self) -> int:
        self.r.zremrangebyscore(OUTSTANDING_ZSET, "-inf", time.time())
        return self.r.zcard(OUTSTANDING_ZSET)

    # --- shared per-client trust (kept simple: read-modify-write; the
    # correctness-critical operations above are the ones made atomic) -----
    def record_violation(self, client_id: str, reason: str):
        key = f"{TRUST_HASH_PREFIX}{client_id}"
        self._decay(key)
        penalty = SUSPICION_PENALTY.get(reason, 0.2)
        self.r.hincrbyfloat(key, "suspicion", penalty)
        self.r.hincrby(key, "violations", 1)
        cur = float(self.r.hget(key, "suspicion") or 0)
        if cur > 1.0:
            self.r.hset(key, "suspicion", 1.0)

    def record_good_request(self, client_id: str):
        key = f"{TRUST_HASH_PREFIX}{client_id}"
        self._decay(key)
        cur = float(self.r.hget(key, "suspicion") or 0)
        self.r.hset(key, "suspicion", max(0.0, cur - 0.02))

    def _decay(self, key: str):
        last_update = float(self.r.hget(key, "last_update") or time.time())
        now = time.time()
        elapsed = now - last_update
        cur = float(self.r.hget(key, "suspicion") or 0)
        self.r.hset(key, "suspicion", max(0.0, cur - elapsed * SUSPICION_DECAY_PER_SEC))
        self.r.hset(key, "last_update", now)

    def is_blocked(self, client_id: str) -> bool:
        key = f"{TRUST_HASH_PREFIX}{client_id}"
        self._decay(key)
        return float(self.r.hget(key, "suspicion") or 0) >= BLOCK_THRESHOLD

    def credit_multiplier(self, client_id: str) -> float:
        key = f"{TRUST_HASH_PREFIX}{client_id}"
        self._decay(key)
        s = float(self.r.hget(key, "suspicion") or 0)
        return max(0.1, 1.0 - s)

    def trust_snapshot(self) -> dict:
        out = {}
        for key in self.r.scan_iter(f"{TRUST_HASH_PREFIX}*"):
            client_id = key[len(TRUST_HASH_PREFIX):]
            h = self.r.hgetall(key)
            suspicion = float(h.get("suspicion", 0))
            out[client_id] = {
                "suspicion": round(suspicion, 3),
                "violations": int(h.get("violations", 0)),
                "blocked": suspicion >= BLOCK_THRESHOLD,
            }
        return out

    def reset(self):
        for key_pattern in ["gateway:*"]:
            for key in self.r.scan_iter(key_pattern):
                self.r.delete(key)
