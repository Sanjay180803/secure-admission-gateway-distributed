"""
Same interface as DistributedLedger, but state lives in this one process's
memory -- exactly what the single-instance version of this project did.

This exists so the demo can run N replicas in "isolated" mode (each with
its own local budget) side by side with N replicas in "coordinated" mode
(sharing the Redis ledger), and show concretely what goes wrong without
coordination: each isolated replica thinks it has a full budget's worth of
headroom, so N replicas collectively over-admit by roughly Nx.
"""
import time
import uuid


class LocalLedger:
    def __init__(self, **_ignored):
        self._outstanding: dict[str, float] = {}
        self._spent: set[str] = set()
        self._ewma = 0.0
        self._budget = 10
        self._trust: dict[str, dict] = {}

    def _purge(self):
        now = time.time()
        for n in [n for n, exp in self._outstanding.items() if exp < now]:
            del self._outstanding[n]

    def reserve_credits(self, requested: int, ttl_seconds: float) -> list[str]:
        self._purge()
        available = max(0, self._budget - len(self._outstanding))
        to_issue = min(available, requested)
        now = time.time()
        issued = []
        for _ in range(to_issue):
            nonce = uuid.uuid4().hex
            self._outstanding[nonce] = now + ttl_seconds
            issued.append(nonce)
        return issued

    def spend_credit(self, nonce: str) -> bool:
        first_time = nonce not in self._spent
        self._spent.add(nonce)
        self._outstanding.pop(nonce, None)
        return first_time

    def is_already_spent(self, nonce: str) -> bool:
        return nonce in self._spent

    def record_delay_and_get_state(self, delay_ms: float):
        self._ewma = 0.3 * delay_ms + 0.7 * self._ewma
        if self._ewma > 20.0:
            self._budget = max(1, int(self._budget * 0.7))
        else:
            self._budget = min(10, self._budget + 2)
        return self._ewma, self._budget

    def current_state(self):
        return self._ewma, self._budget

    def outstanding_count(self) -> int:
        self._purge()
        return len(self._outstanding)

    def _get_trust(self, client_id):
        return self._trust.setdefault(client_id, {"suspicion": 0.0, "violations": 0, "last_update": time.time()})

    def record_violation(self, client_id: str, reason: str):
        penalty = {"bad_signature": 0.40, "replayed": 0.35, "malformed_token": 0.30, "expired": 0.05}.get(reason, 0.2)
        t = self._get_trust(client_id)
        t["suspicion"] = min(1.0, t["suspicion"] + penalty)
        t["violations"] += 1

    def record_good_request(self, client_id: str):
        t = self._get_trust(client_id)
        t["suspicion"] = max(0.0, t["suspicion"] - 0.02)

    def is_blocked(self, client_id: str) -> bool:
        return self._get_trust(client_id)["suspicion"] >= 0.8

    def credit_multiplier(self, client_id: str) -> float:
        return max(0.1, 1.0 - self._get_trust(client_id)["suspicion"])

    def trust_snapshot(self) -> dict:
        return {
            cid: {"suspicion": round(t["suspicion"], 3), "violations": t["violations"], "blocked": t["suspicion"] >= 0.8}
            for cid, t in self._trust.items()
        }

    def reset(self):
        self.__init__()
