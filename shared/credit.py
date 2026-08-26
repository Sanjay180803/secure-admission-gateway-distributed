"""
A "credit" is a signed, single-use, time-limited permission to make one
request. The server issues these; the security properties we need are:

  1. Unforgeable  -- a client can't mint its own credit (HMAC signature
     over server-only secret).
  2. Non-replayable -- a credit can only be spent once (nonce, tracked
     server-side once used).
  3. Expiring -- a credit issued now is worthless in 2 minutes, so a
     client can't hoard credits and burst them all at once later.
"""
import base64
import hashlib
import hmac
import json
import time
import uuid

CREDIT_TTL_SECONDS = 5.0


def _sign(secret: str, payload: str) -> str:
    return hmac.new(secret.encode(), payload.encode(), hashlib.sha256).hexdigest()


def peek_body(token_b64: str) -> dict | None:
    """Decode a credit's body WITHOUT verifying signature -- used only for
    bookkeeping (e.g. tracking outstanding-but-unspent credits by nonce/
    expiry). Never trust this for authorization decisions."""
    try:
        return json.loads(base64.b64decode(token_b64))["body"]
    except Exception:
        return None


def issue_credit(client_id: str, server_secret: str) -> str:
    """Returns a base64 credit token: {client_id, nonce, issued_at, expires_at, sig}."""
    body = {
        "client_id": client_id,
        "nonce": uuid.uuid4().hex,
        "issued_at": time.time(),
        "expires_at": time.time() + CREDIT_TTL_SECONDS,
    }
    body_json = json.dumps(body, sort_keys=True)
    sig = _sign(server_secret, body_json)
    token = {"body": body, "sig": sig}
    return base64.b64encode(json.dumps(token).encode()).decode()


def issue_credit_with_nonce(client_id: str, server_secret: str, nonce: str, expires_at: float) -> str:
    """
    Same as issue_credit, but uses a nonce that was already atomically
    reserved elsewhere (the distributed ledger) rather than generating a
    fresh one -- this is what lets multiple replicas issue credits whose
    identity was agreed on by the shared coordination layer, not just
    locally by whichever replica happens to handle the request.
    """
    body = {
        "client_id": client_id,
        "nonce": nonce,
        "issued_at": time.time(),
        "expires_at": expires_at,
    }
    body_json = json.dumps(body, sort_keys=True)
    sig = _sign(server_secret, body_json)
    token = {"body": body, "sig": sig}
    return base64.b64encode(json.dumps(token).encode()).decode()


class CreditError(Exception):
    pass


def verify_and_consume(token_b64: str, server_secret: str, used_nonces: set) -> dict:
    """
    Verifies signature + expiry + freshness (not-yet-used nonce).
    Raises CreditError with a specific reason on any failure -- caller
    uses the reason to decide how harshly to penalize the client (a
    bad signature is more suspicious than a merely-expired credit).
    On success, marks the nonce as spent so it can never be reused.
    """
    try:
        token = json.loads(base64.b64decode(token_b64))
        body = token["body"]
        sig = token["sig"]
    except Exception:
        raise CreditError("malformed_token")

    body_json = json.dumps(body, sort_keys=True)
    expected_sig = _sign(server_secret, body_json)
    if not hmac.compare_digest(expected_sig, sig):
        raise CreditError("bad_signature")  # most suspicious -- looks like forgery

    if body["client_id"] is None:
        raise CreditError("malformed_token")

    if time.time() > body["expires_at"]:
        raise CreditError("expired")  # less suspicious -- could just be slow client

    if body["nonce"] in used_nonces:
        raise CreditError("replayed")  # very suspicious -- reused a spent credit

    used_nonces.add(body["nonce"])
    return body
