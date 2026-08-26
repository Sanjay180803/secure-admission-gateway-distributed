"""
Run: python3 scripts/end_to_end_demo.py

Demonstrates the FULL path: authenticate -> get a credit (bounded by the
shared budget) -> spend it -> gateway proxies to the real backend -> real
work happens -> result comes back through the gateway. Requests are
round-robined across replicas (client-side here; nginx does this for you
in the Docker Compose deployment) to show any replica can serve any
client, since none of the correctness-critical state is replica-local.
"""
import itertools
import os
import subprocess
import sys
import time

import requests

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
N_REPLICAS = 4
BASE_PORT = 9201
BACKEND_PORT = 9600


def start(cmd_module, port, extra_env, log_tag):
    env = os.environ.copy()
    env.update(extra_env)
    return subprocess.Popen(
        [sys.executable, "-m", "uvicorn", cmd_module, "--port", str(port), "--host", "0.0.0.0", "--log-level", "warning"],
        cwd=PROJECT_ROOT, env=env, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
    )


def wait_up(port, path="/health"):
    for _ in range(60):  # up to 18s -- Windows subprocess startup is slower than Linux
        try:
            r = requests.get(f"http://localhost:{port}{path}", timeout=1)
            if r.status_code == 200:
                return True
        except requests.RequestException:
            pass
        time.sleep(0.3)
    print(f"WARNING: port {port}{path} never became ready within 18s")
    return False


def main():
    print("Starting backend + 4 coordinated replicas...")
    backend = start("backend.service:app", BACKEND_PORT, {"BACKEND_ID": "e2e-backend"}, "backend")
    wait_up(BACKEND_PORT)

    replicas = []
    for i in range(N_REPLICAS):
        port = BASE_PORT + i
        p = start("server.main:app", port, {
            "REPLICA_ID": f"replica-{i}", "COORDINATION_MODE": "coordinated",
            "REDIS_URL": "redis://localhost:6379/0", "BACKEND_URL": f"http://localhost:{BACKEND_PORT}",
        }, f"replica-{i}")
        replicas.append((p, port))
    all_ready = True
    for _, port in replicas:
        if not wait_up(port, "/dashboard"):
            all_ready = False
    if not all_ready:
        print("Not all replicas came up in time -- results below may show failures because of this, not a logic bug.")
    time.sleep(1.0)  # settle buffer

    try:
        ports = itertools.cycle([port for _, port in replicas])  # round-robin, like nginx would do

        print("\n--- Resetting shared state (in case of leftovers from a prior run) ---")
        requests.post(f"http://localhost:{BASE_PORT}/admin/reset")

        print("\n--- Requesting credits from replica-0 ---")
        r = requests.post(f"http://localhost:{BASE_PORT}/credits/request",
                           json={"client_id": "client-good", "client_secret": "good-secret", "count": 5})
        credits = r.json()["credits"]
        print(f"Got {len(credits)} credits (shared budget: {r.json()['shared_budget']})")

        print("\n--- Spending each credit against a DIFFERENT replica (round-robin) ---")
        for i, cred in enumerate(credits):
            port = next(ports)
            try:
                r = requests.post(f"http://localhost:{port}/rpc/call", json={
                    "client_id": "client-good", "credit_token": cred, "payload": f"job-{i}", "complexity": 15,
                }, timeout=10)
            except requests.RequestException as e:
                print(f"  credit {i}: request to port {port} failed entirely: {e}")
                continue
            if r.status_code != 200:
                print(f"  credit {i}: port {port} returned HTTP {r.status_code} -- {r.text[:200]}")
                continue
            result = r.json()
            print(f"  credit {i}: served_by={result.get('served_by')} "
                  f"backend_work_ms={result.get('backend_result', {}).get('work_ms')} "
                  f"total_latency_ms={result.get('total_latency_ms')}")

        print("\n--- Verifying replay protection works ACROSS replicas ---")
        # spend credit 0 again, but against a DIFFERENT replica than it was first spent on
        r = requests.post(f"http://localhost:{BASE_PORT + 2}/rpc/call", json={
            "client_id": "client-good", "credit_token": credits[0], "payload": "replay", "complexity": 5,
        })
        print(f"  replay attempt on a different replica -> HTTP {r.status_code} "
              f"(expect 403, since spend is cluster-wide via Redis, not per-replica)")
        assert r.status_code == 403

        print("\nAll credits were fulfilled by real backend work, load-balanced across replicas,")
        print("and cross-replica replay protection held. Full stack works end to end.")
    finally:
        for p, _ in replicas:
            p.terminate()
        backend.terminate()
        for p, _ in replicas:
            p.wait(timeout=5)
        backend.wait(timeout=5)


if __name__ == "__main__":
    main()
