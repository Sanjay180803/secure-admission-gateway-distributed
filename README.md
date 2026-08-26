# Distributed Secure Admission-Controlled RPC Gateway

The multi-replica evolution of the single-instance admission-control
gateway, now with: a real protected backend (not fake work), nginx load
balancing across replicas, and persistent Redis-backed coordination state.

## What's in this version
- **Real backend** (`backend/service.py`): genuinely variable-cost CPU work
  (SHA-256 rounds proportional to a `complexity` parameter), not a fake
  `sleep()` -- admission control has something honest to protect.
- **nginx load balancing** (`nginx/nginx.conf`): a single entrypoint that
  round-robins across all 4 replicas -- clients never need to know which
  replica handled their request.
- **Redis persistence** (`--appendonly yes` + a named volume in
  `docker-compose.yml`): the shared ledger survives a Redis container
  restart, not just a replica restart (replica restarts already survived
  this even before, since none of the correctness-critical state lived in
  replica memory to begin with).
- Everything from the previous version: atomic cross-replica credit
  reservation, exactly-once spend via `SETNX`, signed/expiring credits,
  trust-based throttling of misbehaving clients, fail-closed auth.

## The problem this solves, concretely
A single gateway instance can track "how many credits are currently
outstanding" in its own memory. The moment you run N replicas for
availability, each replica has its own separate memory -- so each one
independently thinks it has headroom, and collectively they admit up to
**Nx** the intended system-wide capacity. This is a real, general failure
mode (the same shape of bug applies to any "just add more instances"
scaling of something with a global limit: rate limiters, inventory counts,
seat reservations, etc).

## The fix
Push the state that MUST be exact -- outstanding credit count, the
adaptive budget, spent-nonce tracking, per-client trust -- into Redis, and
make the two operations that matter most **atomic** via Lua scripts run
inside Redis itself:
- **Reserve N credit slots**: purge expired reservations, compute
  remaining capacity, and reserve up to N of the requested slots, all in
  one atomic script. No two replicas can race each other into
  over-reserving, because Redis executes the whole script as one
  indivisible operation even under concurrent calls from different
  replicas.
- **Spend a credit exactly once**: `SETNX` on the credit's nonce is
  atomic across every replica talking to the same Redis -- whichever
  replica processes the spend first wins, and it's cluster-wide, not
  per-process.

## Proof this actually matters (not just claimed)

**Over-admission without coordination, measured:** `scripts/multi_replica_demo.py`
spins up 4 real replica processes + a real backend, twice:
- **`COORDINATION_MODE=isolated`** (each replica tracks its own local
  budget) -- measured: **40 credits issued against an intended budget of
  10** (4 replicas x ~10 each = exactly the predicted 4x over-admission).
- **`COORDINATION_MODE=coordinated`** (shared Redis ledger) -- measured:
  **exactly 10 credits issued**, no matter how many replicas or how much
  concurrent load.

**Concurrency stress test:** `tests/test_distributed.py` runs 20
concurrent connections racing to reserve credits against a budget of 10 --
total issued never exceeds 10, proving atomicity under real concurrency.

**Full end-to-end path, across replicas:** `scripts/end_to_end_demo.py`
requests credits from one replica, spends each one against a *different*
replica (simulating what nginx's round-robin does), confirms each hits
the real backend and gets genuine work done, then attempts to replay an
already-spent credit against yet another replica -- correctly rejected
with a 403, proving replay protection is cluster-wide via Redis, not
scoped to whichever replica originally saw the credit.

## Benchmark results (measured, reproducible)

`scripts/benchmark_report.py` runs every quantitative claim this project
makes in one pass (~60-90s) and writes `benchmark_results.json`. These are
real numbers from an actual run, not estimates:

| Metric | Result |
|---|---|
| Backend work proportionality | 5.4ms &rarr; 297ms as `complexity` scaled 2&rarr;80 (real, scaling CPU cost -- not a mock) |
| Concurrency safety (20-way race) | 10/10 issued, **0 over-issued** against a budget of 10 |
| Coordination overhead per request | **+1.5ms** (Redis round-trip vs. in-memory reservation) |
| Over-admission ratio (headline result) | **4.0x** -- 40 credits issued (isolated) vs. 10 (coordinated), same budget |
| Violations to trigger a client block | **3** bad-signature attempts |
| Replay rejection rate | **10/10 correct** -- 0 false accepts, 0 false rejects, under concurrent double-spend attempts |

**The headline tradeoff:** coordination costs about **1.5ms extra per
request** in exchange for eliminating a **proven 4x over-admission bug**
-- a small, quantified cost for a real correctness guarantee, not a
design choice taken on faith.

Run it yourself:
```bash
python3 scripts/benchmark_report.py
```

## Predictive capacity planning (`capacity_planning/`)

The live system's budget control is purely **reactive**: it only shrinks
or grows the budget after measured delay has already moved. This module
adds a **predictive** layer that learns a time-of-day traffic profile from
historical data and sets the budget ahead of a known recurring peak,
instead of waiting to feel the pain first -- turning "job behavior" (past
request-rate patterns) into a decision the platform acts on proactively.

**Four strategies compared via a queueing simulation** (M/M/1 approximation
against a synthetic day with a realistic recurring midday peak):

| Strategy | Mean delay | p99 delay | Overload ticks | Rejected traffic |
|---|---|---|---|---|
| Fixed budget (no adaptation) | 6.9ms | 11.6ms | 0 | **9.5%** (silently under-provisioned for the peak) |
| Reactive (the live system's actual control law) | 11.5ms | 69.2ms | 108 | 3.4% |
| Predictive (seasonal forecast alone) | 18.1ms | 133.8ms | 249 | 0.9% |
| **Hybrid (prediction as baseline + reactive correction on top)** | **11.4ms** | **44.4ms** | **99** | 2.4% |

**Honest finding, not a clean win for prediction alone:** pure prediction
reduces unnecessary rejection (it knows the peak is coming, so it doesn't
over-throttle) -- but it can under-react if the forecast is even slightly
off, since it commits to a budget for the whole predicted window with no
self-correction. That shows up as *more* overload ticks than reactive
alone, not fewer. The fix was combining both: **hybrid uses the seasonal
prediction as a baseline and still applies bounded reactive correction on
top**, which gets the best p99 latency and fewest overload ticks of all
four strategies, with rejection between the two pure approaches.

**On a day with a genuine, unpredictable anomaly** (a spike with no
relationship to the daily pattern), all strategies converge toward
reactive-like behavior for the anomaly itself, since no forecast saw it
coming -- this is included deliberately: prediction helps with *known*
patterns, reactive correction remains the necessary safety net for
genuine surprises, and the results don't pretend otherwise.

Run it yourself:
```bash
python3 capacity_planning/simulate.py
```

## Run it

### Full stack via Docker Compose (real deployment shape: nginx + 4 replicas + backend + persistent Redis)
```bash
docker compose up --build
curl localhost:8000/dashboard        # through nginx -- hits whichever replica it routes to
curl localhost:9001/dashboard        # replica-a directly, for debugging
curl localhost:9002/dashboard        # replica-b directly -- same shared_budget as 9001, proving coordination
curl localhost:7000/stats            # the real backend's own request count
```

### Natively (no Docker), for iterating on the code
**Requires a running Redis:**
```bash
redis-server --daemonize yes
```

**Install deps:**
```bash
pip install -r requirements.txt
```

**Correctness tests (ledger atomicity, exactly-once spend, blocking):**
```bash
python3 tests/test_distributed.py
```

**Isolated vs. coordinated proof (the headline demo):**
```bash
python3 scripts/multi_replica_demo.py
```

**Full end-to-end path (real backend + cross-replica credit spending + cross-replica replay rejection):**
```bash
python3 scripts/end_to_end_demo.py
```

**Full benchmark report (every metric above, in one run):**
```bash
python3 scripts/benchmark_report.py
```

## What's reused from the single-instance version
The credit token format (`shared/credit.py`) and the security properties
(signed, single-use, expiring credits; trust-based throttling of
misbehaving clients; fail-closed auth) are unchanged -- this version adds
the distributed-systems layer (coordination, load balancing, a real
backend, persistence) on top, it doesn't replace the security layer.

## Architecture
```
                        clients
                           │
                           ▼
                    ┌────────────┐
                    │   nginx    │  round-robin
                    └──────┬─────┘
        ┌──────────┬───────┼───────┬──────────┐
        ▼          ▼       ▼       ▼          │
   replica-a  replica-b replica-c replica-d    │
        │          │       │       │           │
        └──────────┴───┬───┴───────┘           │
                        ▼                       │
                 ┌────────────┐          all replicas
                 │Redis(ledger)│ ◄────── coordinate through
                 │  (AOF vol) │          this shared state
                 └────────────┘
                        │
                        ▼ (only admitted requests)
                 ┌────────────┐
                 │  backend   │  real protected work
                 └────────────┘
```

## Reference
Cho, I., Saeed, A., Fried, J., Park, S. J., Alizadeh, M., & Belay, A. (2020).
*Overload Control for μs-scale RPCs with Breakwater*. OSDI'20.

