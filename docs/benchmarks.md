# Benchmark methodology

All numbers come from `scripts/benchmark_report.py`, which starts its own
backend and replica processes (uvicorn) against a local Redis, runs six
measurements, and writes `benchmark_results.json`.

```bash
redis-server --daemonize yes
pip install -r requirements.txt
python3 scripts/benchmark_report.py     # ~90 s
```

## Environment for the results below

- 2 vCPU Intel Xeon @ 2.10 GHz, Linux, Python 3.13
- Redis 7 on the same host (loopback), Redis DB flushed before each run
- 4 runs on 2026-10-23

## What each section measures

| Section | Procedure | Metric |
|---|---|---|
| A. Backend cost | One `/work` call at `complexity` 2, 10, 30, 50, 80 | `work_ms` reported by the backend |
| B. Concurrency safety | 20 threads, each calling `reserve_credits(3)` on its own `DistributedLedger`, budget 10 | credits issued |
| C. Coordination overhead | 50 sequential reserve+spend cycles, in-memory `LocalLedger` vs. Redis `DistributedLedger` | p50 latency of the reserve call |
| D. Over-admission | 4 replica processes, budget 10. 32 concurrent `/credits/request` calls (8 per replica, `count=3`). Run once with `COORDINATION_MODE=isolated`, once with `coordinated` | total credits issued |
| E. Blocking | Repeated `bad_signature` violations for one client | violations until `is_blocked` |
| F. Replay rejection | One replica; 10 credits, each spent twice concurrently (20 calls) | accepted vs. rejected |

## Results

| Metric | Run 1 | Run 2 | Run 3 | Run 4 |
|---|---:|---:|---:|---:|
| D. credits issued, isolated (budget 10) | 41 | 40 | 40 | 40 |
| D. credits issued, coordinated (budget 10) | 10 | 10 | 10 | 10 |
| B. credits issued in 20-way race (budget 10) | 10 | 10 | 10 | 10 |
| C. coordinated reserve p50 (ms) | 0.087 | 0.093 | 0.107 | 0.094 |
| C. overhead vs. in-memory (ms) | 0.084 | 0.090 | 0.103 | 0.091 |
| E. violations to block | 3 | 3 | 3 | 3 |
| F. replays accepted / rejected | 10 / 10 | 10 / 10 | 10 / 10 | 10 / 10 |
| A. backend work_ms at complexity 80 | 50.4 | 78.9 | 51.9 | 50.5 |

## Caveats

- Section C measures a Redis round trip over loopback. Across a network,
  expect the overhead to be about one network RTT (an earlier run on a
  laptop measured +1.5 ms).
- Section D's isolated result depends on how requests spread across
  replicas; the expected value is replicas × budget = 40.
- Sections B and F test at most 20 concurrent operations. They show the
  atomic scripts behave correctly under contention at that scale; they are
  not throughput benchmarks.
- Section A is single-shot; run-to-run variation at high complexity
  (50–79 ms at complexity 80) is CPU scheduling noise on a 2-vCPU host.
