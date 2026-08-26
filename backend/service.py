"""
The actual service being protected by the admission-control gateway.

This does REAL work with a genuine, variable cost -- not a fixed sleep --
so admission control has something honest to protect against: a request
with `complexity=50` costs meaningfully more than `complexity=5`, and if
enough expensive requests land at once, this service's own capacity
becomes the real bottleneck the gateway is shielding.

Run standalone: uvicorn backend.service:app --port 7000
"""
import hashlib
import os
import time

from fastapi import FastAPI
from pydantic import BaseModel

BACKEND_ID = os.environ.get("BACKEND_ID", "backend-1")
MAX_CONCURRENT_WORK = int(os.environ.get("MAX_CONCURRENT_WORK", "5"))

app = FastAPI(title=f"protected-backend[{BACKEND_ID}]")

import asyncio
_work_semaphore = asyncio.Semaphore(MAX_CONCURRENT_WORK)
_stats = {"requests_served": 0, "total_work_ms": 0.0}


class WorkRequest(BaseModel):
    payload: str = "default"
    complexity: int = 10  # higher = more CPU-bound hashing rounds -> real variable cost


@app.post("/work")
async def do_work(req: WorkRequest):
    async with _work_semaphore:
        t0 = time.perf_counter()
        # genuine CPU-bound cost proportional to `complexity` -- not a sleep
        digest = req.payload.encode()
        for _ in range(req.complexity * 2000):
            digest = hashlib.sha256(digest).digest()
        elapsed_ms = (time.perf_counter() - t0) * 1000

    _stats["requests_served"] += 1
    _stats["total_work_ms"] += elapsed_ms
    return {
        "backend_id": BACKEND_ID,
        "result_digest": digest.hex()[:16],
        "work_ms": round(elapsed_ms, 2),
    }


@app.get("/health")
def health():
    return {"backend_id": BACKEND_ID, "status": "ok"}


@app.get("/stats")
def stats():
    return {"backend_id": BACKEND_ID, **_stats}
