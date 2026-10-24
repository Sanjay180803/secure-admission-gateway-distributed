"""
HTTP-level tests for a gateway replica, with the protected backend stubbed
out so the tests exercise admission logic only.
"""
import base64
import json

import httpx
import pytest
from fastapi.testclient import TestClient

from conftest import requires_redis
from shared.ledger import DistributedLedger

pytestmark = requires_redis

GOOD = {"client_id": "client-good", "client_secret": "good-secret"}


def _backend_stub(request: httpx.Request) -> httpx.Response:
    return httpx.Response(200, json={"backend_id": "stub", "result_digest": "0" * 16, "work_ms": 0.1})


@pytest.fixture
def client(ledger, monkeypatch):
    from server import main

    monkeypatch.setattr(main, "ledger", ledger)
    monkeypatch.setattr(main, "_http_client", httpx.AsyncClient(transport=httpx.MockTransport(_backend_stub)))
    return TestClient(main.app, raise_server_exceptions=False)


def _get_credits(client, count=3):
    resp = client.post("/credits/request", json={**GOOD, "count": count})
    assert resp.status_code == 200
    return resp.json()["credits"]


def _call(client, token):
    return client.post("/rpc/call", json={"client_id": "client-good", "credit_token": token, "complexity": 1})


def test_valid_credit_is_admitted(client):
    (token,) = _get_credits(client, 1)
    resp = _call(client, token)
    assert resp.status_code == 200
    assert resp.json()["status"] == "ok"


def test_replayed_credit_is_rejected(client):
    (token,) = _get_credits(client, 1)
    assert _call(client, token).status_code == 200
    resp = _call(client, token)
    assert resp.status_code == 403
    assert "replayed" in resp.json()["detail"]


def test_missing_credit_is_rejected(client):
    resp = client.post("/rpc/call", json={"client_id": "client-good"})
    assert resp.status_code == 403


def test_tampered_credit_is_rejected(client):
    (token,) = _get_credits(client, 1)
    decoded = json.loads(base64.b64decode(token))
    decoded["body"]["expires_at"] += 3600
    forged = base64.b64encode(json.dumps(decoded).encode()).decode()
    resp = _call(client, forged)
    assert resp.status_code == 403
    assert "bad_signature" in resp.json()["detail"]


def test_unknown_client_cannot_request_credits(client):
    resp = client.post("/credits/request", json={"client_id": "client-good", "client_secret": "wrong"})
    assert resp.status_code == 401


def test_credit_issuance_respects_shared_budget(client):
    issued = sum(len(_get_credits(client, 5)) for _ in range(5))
    assert issued == 10


def test_redis_unavailable_fails_closed(client, monkeypatch):
    """With Redis down, the replica must not issue credits or admit requests."""
    from server import main

    (token,) = _get_credits(client, 1)
    dead = DistributedLedger("redis://localhost:6399/0")
    monkeypatch.setattr(main, "ledger", dead)

    assert client.post("/credits/request", json={**GOOD, "count": 1}).status_code >= 500
    assert _call(client, token).status_code >= 500
