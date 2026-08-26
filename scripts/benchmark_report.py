"""
Run: python3 scripts/benchmark_report.py

Runs every measurable claim this project makes and prints a single summary
table at the end. Spins up its own backend + replica processes (needs a
reachable Redis at localhost:6379, same as the other scripts).

Takes about 60-90 seconds total.
"""
import base64
import concurrent.futures
import json
import os
import statistics
import subprocess
import sys
import time

import requests

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, PROJECT_ROOT)

BACKEND_PORT = 9700
BASE_PORT = 9301
N_REPLICAS = 4

results = {}  # collected metrics, printed as a table at the end


def start(cmd_module, port, extra_env):
    env = os.environ.copy()
    env.update(extra_env)
    return subprocess.Popen(
        [sys.executable, "-m", "uvicorn", cmd_module, "--port", str(port), "--host", "0.0.0.0", "--log-level", "warning"],
        cwd=PROJECT_ROOT, env=env, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
    )


def wait_up(port, path="/health", timeout_s=18):
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        try:
            r = requests.get(f"http://localhost:{port}{path}", timeout=1)
            if r.status_code == 200:
                return True
        except requests.RequestException:
            pass
        time.sleep(0.3)
    return False


def stop(procs):
    for p in procs:
        p.terminate()
    for p in procs:
        try:
            p.wait(timeout=5)
        except subprocess.TimeoutExpired:
            p.kill()


# ---------------------------------------------------------------------------
print("=== Section A: Backend work proportionality ===")
backend = start("backend.service:app", BACKEND_PORT, {"BACKEND_ID": "bench-backend"})
wait_up(BACKEND_PORT)
try:
    proportionality = []
    for complexity in [2, 10, 30, 50, 80]:
        r = requests.post(f"http://localhost:{BACKEND_PORT}/work",
                           json={"payload": "bench", "complexity": complexity}, timeout=10)
        work_ms = r.json()["work_ms"]
        proportionality.append((complexity, work_ms))
        print(f"  complexity={complexity:>3} -> work_ms={work_ms}")
    results["backend_proportionality"] = proportionality
finally:
    backend.terminate()
    backend.wait(timeout=5)

# ---------------------------------------------------------------------------
print("\n=== Section B: Concurrency safety (20-way race, budget=10) ===")
from shared.ledger import DistributedLedger  # noqa: E402
ledger = DistributedLedger()
ledger.reset()

def try_reserve():
    return DistributedLedger().reserve_credits(3, 10.0)

with concurrent.futures.ThreadPoolExecutor(max_workers=20) as ex:
    race_results = list(ex.map(lambda _: try_reserve(), range(20)))
total_issued = sum(len(r) for r in race_results)
over_issued = max(0, total_issued - 10)
print(f"  {total_issued} credits issued against a budget of 10 (over-issued: {over_issued})")
results["concurrency_race_issued"] = total_issued
results["concurrency_race_over_issued"] = over_issued

# ---------------------------------------------------------------------------
print("\n=== Section C: Atomicity overhead (isolated vs coordinated reserve latency) ===")
from shared.local_ledger import LocalLedger  # noqa: E402
local = LocalLedger()
local_latencies = []
for _ in range(50):
    t0 = time.perf_counter()
    local.reserve_credits(1, 10.0)
    local_latencies.append((time.perf_counter() - t0) * 1000)
    local.spend_credit(list(local._outstanding.keys())[0]) if local._outstanding else None

ledger.reset()
dist_latencies = []
for _ in range(50):
    t0 = time.perf_counter()
    nonces = ledger.reserve_credits(1, 10.0)
    dist_latencies.append((time.perf_counter() - t0) * 1000)
    if nonces:
        ledger.spend_credit(nonces[0])

local_p50 = statistics.median(local_latencies)
dist_p50 = statistics.median(dist_latencies)
print(f"  isolated (in-memory):    p50={local_p50:.3f}ms")
print(f"  coordinated (redis):     p50={dist_p50:.3f}ms")
print(f"  coordination overhead:   +{dist_p50 - local_p50:.3f}ms per request")
results["isolated_reserve_p50_ms"] = round(local_p50, 3)
results["coordinated_reserve_p50_ms"] = round(dist_p50, 3)
results["coordination_overhead_ms"] = round(dist_p50 - local_p50, 3)

# ---------------------------------------------------------------------------
print("\n=== Section D: Isolated vs coordinated over-admission (headline result) ===")

def run_admission_scenario(mode):
    ledger.reset()
    procs, ports = [], []
    for i in range(N_REPLICAS):
        port = BASE_PORT + i
        p = start("server.main:app", port, {
            "REPLICA_ID": f"bench-{i}", "COORDINATION_MODE": mode,
            "REDIS_URL": "redis://localhost:6379/0", "BACKEND_URL": f"http://localhost:{BACKEND_PORT}",
        })
        procs.append(p)
        ports.append(port)
    try:
        for port in ports:
            wait_up(port, "/dashboard")
        time.sleep(0.5)
        for port in ports:
            try:
                requests.post(f"http://localhost:{port}/admin/reset", timeout=3)
            except requests.RequestException:
                pass

        def ask(port):
            try:
                r = requests.post(f"http://localhost:{port}/credits/request",
                                   json={"client_id": "client-good", "client_secret": "good-secret", "count": 3},
                                   timeout=5)
                return r.json().get("count", 0) if r.status_code == 200 else 0
            except requests.RequestException:
                return 0

        tasks = [p for p in ports for _ in range(8)]
        with concurrent.futures.ThreadPoolExecutor(max_workers=len(tasks)) as ex:
            total = sum(ex.map(ask, tasks))
        return total
    finally:
        stop(procs)


backend = start("backend.service:app", BACKEND_PORT, {"BACKEND_ID": "bench-backend-2"})
wait_up(BACKEND_PORT)
try:
    isolated_total = run_admission_scenario("isolated")
    print(f"  isolated:    {isolated_total} credits issued (budget=10)")
    time.sleep(1)
    coordinated_total = run_admission_scenario("coordinated")
    print(f"  coordinated: {coordinated_total} credits issued (budget=10)")
    results["isolated_total_issued"] = isolated_total
    results["coordinated_total_issued"] = coordinated_total
    results["over_admission_ratio"] = round(isolated_total / 10, 1)
finally:
    backend.terminate()
    backend.wait(timeout=5)

# ---------------------------------------------------------------------------
print("\n=== Section E: Trust/block timing (how many violations to trigger a block) ===")
ledger.reset()
violations_to_block = 0
for i in range(1, 30):
    ledger.record_violation("bench-bad-client", "bad_signature")
    violations_to_block = i
    if ledger.is_blocked("bench-bad-client"):
        break
print(f"  client blocked after {violations_to_block} bad-signature violations")
results["violations_to_block"] = violations_to_block

# ---------------------------------------------------------------------------
print("\n=== Section F: Replay rejection rate ===")
backend = start("backend.service:app", BACKEND_PORT, {"BACKEND_ID": "bench-backend-3"})
wait_up(BACKEND_PORT)
replica = start("server.main:app", BASE_PORT, {
    "REPLICA_ID": "bench-replay", "COORDINATION_MODE": "coordinated",
    "REDIS_URL": "redis://localhost:6379/0", "BACKEND_URL": f"http://localhost:{BACKEND_PORT}",
})
try:
    wait_up(BASE_PORT, "/dashboard")
    time.sleep(0.5)
    requests.post(f"http://localhost:{BASE_PORT}/admin/reset")
    r = requests.post(f"http://localhost:{BASE_PORT}/credits/request",
                       json={"client_id": "client-good", "client_secret": "good-secret", "count": 10})
    credits = r.json()["credits"]

    def spend(cred):
        r = requests.post(f"http://localhost:{BASE_PORT}/rpc/call",
                           json={"client_id": "client-good", "credit_token": cred, "complexity": 5}, timeout=10)
        return r.status_code == 200

    # spend each credit TWICE concurrently -- exactly one of each pair should win
    tasks = credits + credits
    with concurrent.futures.ThreadPoolExecutor(max_workers=len(tasks)) as ex:
        outcomes = list(ex.map(spend, tasks))
    successes = sum(outcomes)
    rejections = len(outcomes) - successes
    print(f"  {len(credits)} credits, each spent twice concurrently ({len(tasks)} total attempts)")
    print(f"  successes: {successes} (expected exactly {len(credits)})")
    print(f"  replay rejections: {rejections} (expected exactly {len(credits)})")
    results["replay_test_attempts"] = len(tasks)
    results["replay_test_successes"] = successes
    results["replay_test_correctly_rejected"] = rejections
    results["replay_rejection_correct"] = (successes == len(credits))
finally:
    stop([replica, backend])

# ---------------------------------------------------------------------------
print("\n" + "=" * 60)
print("SUMMARY TABLE")
print("=" * 60)
for k, v in results.items():
    if k == "backend_proportionality":
        continue
    print(f"  {k:35s} {v}")
print("=" * 60)

with open(os.path.join(PROJECT_ROOT, "benchmark_results.json"), "w") as f:
    json.dump(results, f, indent=2)
print(f"\nFull results written to benchmark_results.json")
