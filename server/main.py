"""
A gateway REPLICA. Multiple copies of this process can run behind a load
balancer; when COORDINATION_MODE=coordinated they share one global credit
budget through Redis. In COORDINATION_MODE=isolated, each replica tracks
its own local budget (like the original single-instance project) -- kept
as a mode specifically to demonstrate the over-admission failure that
motivates needing coordination in the first place.

Env vars:
  REPLICA_ID=replica-a
  COORDINATION_MODE=coordinated   # or "isolated"
  REDIS_URL=redis://localhost:6379/0
  SERVER_SECRET=dev-only-secret-change-me
"""
import asyncio
import base64
import hashlib
import hmac
import json
import os
import random
import sys
import time

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import httpx
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

from shared.credit import issue_credit_with_nonce, CreditError
from shared.ledger import DistributedLedger
from shared.local_ledger import LocalLedger

REPLICA_ID = os.environ.get("REPLICA_ID", "replica-unknown")
COORDINATION_MODE = os.environ.get("COORDINATION_MODE", "coordinated")
REDIS_URL = os.environ.get("REDIS_URL", "redis://localhost:6379/0")
SERVER_SECRET = os.environ.get("SERVER_SECRET", "dev-only-secret-change-me")
BACKEND_URL = os.environ.get("BACKEND_URL", "http://localhost:7000")
CLIENT_SECRETS = {"client-good": "good-secret", "client-bad": "bad-secret"}
MAX_CONCURRENT_PROCESSING = 5
CREDIT_TTL_SECONDS = 30.0  # generous window -- real network/process overhead (especially on
                            # Windows, where subprocess + real HTTP round trips run slower than
                            # a fast Linux dev box) can easily eat several seconds across a
                            # handful of sequential requests; 5s was tuned for a fast sandbox
                            # and was too tight for real-world conditions.

app = FastAPI(title=f"gateway-replica[{REPLICA_ID}]")

ledger = DistributedLedger(REDIS_URL) if COORDINATION_MODE == "coordinated" else LocalLedger()
_processing_semaphore = asyncio.Semaphore(MAX_CONCURRENT_PROCESSING)
_http_client = httpx.AsyncClient(timeout=10.0)

_metrics = {"requests_accepted": 0, "requests_rejected_no_credit": 0, "requests_rejected_bad_credit": 0,
            "credits_issued": 0, "credits_denied": 0, "latencies_ms": []}


class CreditRequest(BaseModel):
    client_id: str
    client_secret: str
    count: int = 5


class RpcRequest(BaseModel):
    client_id: str
    credit_token: str | None = None
    payload: str = "default"
    complexity: int = 10


def _verify_structure(token_b64: str) -> dict:
    """
    Signature + expiry check WITHOUT the nonce-replay check -- replay
    detection goes through the shared ledger's atomic spend_credit
    instead, since it must be cluster-wide, not per-replica-in-memory.
    """
    try:
        token = json.loads(base64.b64decode(token_b64))
        body, sig = token["body"], token["sig"]
    except Exception:
        raise CreditError("malformed_token")
    body_json = json.dumps(body, sort_keys=True)
    expected_sig = hmac.new(SERVER_SECRET.encode(), body_json.encode(), hashlib.sha256).hexdigest()
    if not hmac.compare_digest(expected_sig, sig):
        raise CreditError("bad_signature")
    if time.time() > body["expires_at"]:
        raise CreditError("expired")
    return body


@app.post("/credits/request")
def request_credits(req: CreditRequest):
    expected_secret = CLIENT_SECRETS.get(req.client_id)
    if expected_secret is None or req.client_secret != expected_secret:
        raise HTTPException(status_code=401, detail="unknown client or bad secret")

    if ledger.is_blocked(req.client_id):
        _metrics["credits_denied"] += 1
        raise HTTPException(status_code=429, detail="client blocked due to repeated credit violations")

    requested = max(1, int(req.count * ledger.credit_multiplier(req.client_id)))
    reserved_nonces = ledger.reserve_credits(requested, CREDIT_TTL_SECONDS)

    if not reserved_nonces:
        _metrics["credits_denied"] += 1
        ewma, budget = ledger.current_state()
        return {"credits": [], "count": 0, "issuing_replica": REPLICA_ID, "reason": "no capacity available",
                "shared_budget": budget, "shared_queue_delay_ms": round(ewma, 2)}

    expires_at = time.time() + CREDIT_TTL_SECONDS
    credits = [issue_credit_with_nonce(req.client_id, SERVER_SECRET, nonce, expires_at) for nonce in reserved_nonces]
    _metrics["credits_issued"] += len(credits)
    ewma, budget = ledger.current_state()
    return {"credits": credits, "count": len(credits), "issuing_replica": REPLICA_ID,
            "shared_budget": budget, "shared_queue_delay_ms": round(ewma, 2)}


@app.post("/rpc/call")
async def rpc_call(req: RpcRequest):
    arrival_time = time.perf_counter()

    if req.credit_token is None:
        _metrics["requests_rejected_no_credit"] += 1
        raise HTTPException(status_code=403, detail="no credit presented")

    try:
        body = _verify_structure(req.credit_token)
    except CreditError as e:
        ledger.record_violation(req.client_id, str(e))
        _metrics["requests_rejected_bad_credit"] += 1
        raise HTTPException(status_code=403, detail=f"invalid credit: {e}")

    first_time = ledger.spend_credit(body["nonce"])
    if not first_time:
        ledger.record_violation(req.client_id, "replayed")
        _metrics["requests_rejected_bad_credit"] += 1
        raise HTTPException(status_code=403, detail="invalid credit: replayed")

    ledger.record_good_request(req.client_id)

    async with _processing_semaphore:
        queue_delay_ms = (time.perf_counter() - arrival_time) * 1000
        ledger.record_delay_and_get_state(queue_delay_ms)

        # forward the admitted request to the REAL protected backend --
        # this is what admission control was actually gating access to.
        try:
            backend_resp = await _http_client.post(
                f"{BACKEND_URL}/work", json={"payload": req.payload, "complexity": req.complexity}
            )
            backend_resp.raise_for_status()
            backend_result = backend_resp.json()
        except httpx.HTTPError as e:
            raise HTTPException(status_code=502, detail=f"backend call failed: {e}")

    total_latency_ms = (time.perf_counter() - arrival_time) * 1000
    _metrics["requests_accepted"] += 1
    _metrics["latencies_ms"].append(total_latency_ms)
    _metrics["latencies_ms"] = _metrics["latencies_ms"][-500:]

    return {"status": "ok", "served_by": REPLICA_ID, "queue_delay_ms": round(queue_delay_ms, 2),
            "total_latency_ms": round(total_latency_ms, 2), "backend_result": backend_result}


@app.post("/admin/reset")
def admin_reset():
    ledger.reset()
    for k in ("requests_accepted", "requests_rejected_no_credit", "requests_rejected_bad_credit",
              "credits_issued", "credits_denied"):
        _metrics[k] = 0
    _metrics["latencies_ms"] = []
    return {"status": "reset", "replica": REPLICA_ID}


@app.get("/dashboard")
def dashboard():
    lat = sorted(_metrics["latencies_ms"])
    p50 = lat[len(lat) // 2] if lat else 0
    p99 = lat[int(len(lat) * 0.99) - 1] if lat else 0
    ewma, budget = ledger.current_state()
    return {
        "replica_id": REPLICA_ID,
        "coordination_mode": COORDINATION_MODE,
        "shared_ewma_delay_ms": round(ewma, 2),
        "shared_budget": budget,
        "outstanding_credits_visible_to_this_replica": ledger.outstanding_count(),
        "metrics": {**_metrics, "latencies_ms": None, "p50_latency_ms": round(p50, 2), "p99_latency_ms": round(p99, 2)},
        "trust": ledger.trust_snapshot(),
    }
