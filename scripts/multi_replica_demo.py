"""
Run: python3 scripts/multi_replica_demo.py
(spawns real replica processes itself, no manual setup needed except a
running Redis on localhost:6379)

Demonstrates the actual reason coordination is needed: N replicas each
running their own local admission-control state will collectively admit
roughly Nx the intended budget, because none of them can see what the
others have already issued. The coordinated version fixes this by having
every replica check/reserve against one shared Redis-backed ledger.
"""
import concurrent.futures
import os
import subprocess
import sys
import time

import requests

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
N_REPLICAS = 4
BASE_PORT = 9001
BACKEND_PORT = 9500


def start_backend():
    p = subprocess.Popen(
        [sys.executable, "-m", "uvicorn", "backend.service:app", "--port", str(BACKEND_PORT), "--host", "0.0.0.0",
         "--log-level", "warning"],
        cwd=PROJECT_ROOT, env={**os.environ, "BACKEND_ID": "demo-backend"},
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
    )
    for _ in range(30):
        try:
            requests.get(f"http://localhost:{BACKEND_PORT}/health", timeout=1)
            return p
        except requests.RequestException:
            time.sleep(0.3)
    return p


def start_replicas(mode: str):
    procs = []
    for i in range(N_REPLICAS):
        port = BASE_PORT + i
        env = os.environ.copy()
        env.update({
            "REPLICA_ID": f"replica-{i}",
            "COORDINATION_MODE": mode,
            "REDIS_URL": "redis://localhost:6379/0",
            "BACKEND_URL": f"http://localhost:{BACKEND_PORT}",
        })
        p = subprocess.Popen(
            [sys.executable, "-m", "uvicorn", "server.main:app", "--port", str(port), "--host", "0.0.0.0",
             "--log-level", "warning"],
            cwd=PROJECT_ROOT, env=env, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
        )
        procs.append((p, port))
    return procs


def wait_for_replicas(procs):
    for _, port in procs:
        ready = False
        for _ in range(60):  # up to 18s per replica -- Windows subprocess/import startup is slower
            try:
                r = requests.get(f"http://localhost:{port}/dashboard", timeout=1)
                if r.status_code == 200:
                    ready = True
                    break
            except requests.RequestException:
                pass
            time.sleep(0.3)
        if not ready:
            print(f"WARNING: replica on port {port} never became ready within 18s")
    time.sleep(1.0)  # small settle buffer even after all report ready


def reset_all(procs):
    for _, port in procs:
        try:
            r = requests.post(f"http://localhost:{port}/admin/reset", timeout=3)
            if r.status_code != 200:
                print(f"  WARNING: reset on port {port} returned HTTP {r.status_code}")
        except requests.RequestException as e:
            print(f"  WARNING: reset on port {port} failed: {e}")


def sanity_check_modes(procs, expected_mode):
    """Catches a real, seen-in-practice failure mode: a leftover/zombie
    process from a PREVIOUS run still bound to one of these ports, serving
    the wrong coordination mode and silently skewing results."""
    for _, port in procs:
        try:
            d = requests.get(f"http://localhost:{port}/dashboard", timeout=2).json()
            actual_mode = d.get("coordination_mode")
            if actual_mode != expected_mode:
                print(f"  WARNING: port {port} reports mode='{actual_mode}', expected '{expected_mode}' "
                      f"-- likely a leftover process from a previous run still bound to this port!")
        except requests.RequestException as e:
            print(f"  WARNING: could not check mode on port {port}: {e}")


def stop_replicas(procs):
    for p, _ in procs:
        p.terminate()
    for p, _ in procs:
        try:
            p.wait(timeout=5)
        except subprocess.TimeoutExpired:
            p.kill()


def hammer_for_credits(procs, requests_per_replica=8):
    """
    Simulates load-balanced traffic: each of the N replicas independently
    gets hit with credit requests around the same time -- exactly the
    scenario where isolated per-replica budgets diverge from a shared one.
    """
    def ask(port):
        try:
            r = requests.post(
                f"http://localhost:{port}/credits/request",
                json={"client_id": "client-good", "client_secret": "good-secret", "count": 3},
                timeout=5,
            )
            if r.status_code != 200:
                print(f"  [port {port}] credit request failed: HTTP {r.status_code} -- {r.text[:200]}")
                return port, 0
            return port, r.json().get("count", 0)
        except requests.RequestException as e:
            print(f"  [port {port}] credit request errored: {e}")
            return port, 0

    tasks = []
    for _, port in procs:
        tasks.extend([port] * requests_per_replica)

    with concurrent.futures.ThreadPoolExecutor(max_workers=len(tasks)) as ex:
        results = list(ex.map(ask, tasks))

    per_port = {}
    for port, count in results:
        per_port[port] = per_port.get(port, 0) + count
    print("  per-replica breakdown:", per_port)

    return sum(count for _, count in results)


def run_scenario(mode: str):
    print(f"\n=== Mode: {mode} ({N_REPLICAS} replicas, each hit with simultaneous credit requests) ===")
    procs = start_replicas(mode)
    try:
        wait_for_replicas(procs)
        sanity_check_modes(procs, mode)
        reset_all(procs)
        total_issued = hammer_for_credits(procs)
        print(f"Intended system-wide budget: 10 credits")
        print(f"Total credits actually issued across all {N_REPLICAS} replicas: {total_issued}")
        if total_issued > 10:
            print(f"--> OVER-ADMITTED by {total_issued - 10} credits ({total_issued/10:.1f}x intended budget)")
        else:
            print("--> stayed within the intended global budget")
        return total_issued
    finally:
        stop_replicas(procs)


def main():
    print("Starting shared backend service...")
    backend_proc = start_backend()
    try:
        isolated_total = run_scenario("isolated")
        time.sleep(1)
        coordinated_total = run_scenario("coordinated")

        print("\n=== Summary ===")
        print(f"isolated (no coordination):   {isolated_total} credits issued (budget was 10)")
        print(f"coordinated (shared ledger):  {coordinated_total} credits issued (budget was 10)")
    finally:
        backend_proc.terminate()
        try:
            backend_proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            backend_proc.kill()


if __name__ == "__main__":
    main()
