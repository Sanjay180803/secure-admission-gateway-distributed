# Consistency guarantees

This page states what the gateway guarantees, under which assumptions, and
where the guarantees stop. All statements refer to
`COORDINATION_MODE=coordinated` with a single Redis instance.

## Admission vs. execution

The gateway guarantees properties about **admission** (whether a credit may
be used to enter the system). It does **not** guarantee anything about
**execution** in the backend:

- A credit is consumed *before* the backend is called. If the backend call
  fails or times out, the gateway returns 502 and the credit is not
  refunded. The client must request a new credit to retry.
- The backend has no idempotency key. If a client retries with a fresh
  credit after a timeout, the backend may perform the work twice.

So the end-to-end property is: **each credit admits at most one backend
call**, not "each logical request executes exactly once".

## What is atomic

| Operation | Mechanism | Guarantee |
|---|---|---|
| Reserve credits | One Lua script: purge expired entries, count outstanding, compare with budget, add new nonces | Concurrent replicas cannot together reserve more slots than the budget. Tested with 20- and 50-way concurrent reservation. |
| Consume a credit | `SET gateway:spent:{nonce} 1 NX EX 60` | For a given nonce, exactly one replica sees success. Tested with 20 concurrent spends of one credit. |
| Update budget | One Lua script: update EWMA, apply AIMD | Delay samples from different replicas never overwrite each other. |

## What is idempotent

- **Credit consumption.** Presenting the same credit again returns 403
  (`replayed`) on every replica for as long as the spent marker exists.
- **Reservation is not idempotent.** Each call to `/credits/request`
  reserves new slots.

## What is not atomic or transactional

- **Spend then release.** `spend_credit` runs `SET NX` and then `ZREM` as
  two commands. If a replica dies between them, the slot stays counted as
  outstanding until its reservation expires (30 s), temporarily reducing
  capacity. It never over-admits.
- **Client trust scores** use read-modify-write without a script.
  Concurrent violations for the same client can lose an update, so
  suspicion values are approximate. Blocking is a heuristic, not a
  correctness property.
- **Metrics** on `/dashboard` are per replica and in memory.

## Time and expiry

- Credit expiry is checked against the verifying replica's wall clock.
  Replicas need roughly synchronized clocks (NTP).
- Spent markers live for 60 s; credits live for 30 s. A replayed credit is
  rejected either because its marker still exists or because it has
  expired. This holds as long as replica clock skew stays under 30 s.

## Redis failure behavior

| Situation | Behavior |
|---|---|
| Redis unreachable | Credit issuance and RPC calls return 5xx. No request is admitted. The gateway **fails closed**. Covered by `test_redis_unavailable_fails_closed`. |
| Redis restart with AOF | Budget, outstanding reservations, spent markers and trust scores are reloaded from the AOF volume. |
| Redis crash, AOF loss window | With the default `appendfsync everysec`, up to about 1 s of writes can be lost. A credit consumed in that window, if still unexpired, could be accepted once more after restart. Use `appendfsync always` to close this window at a throughput cost. |
| Single point of failure | There is one Redis instance. Sentinel or Cluster is not configured. Under Cluster, the multi-key Lua scripts would need hash-tagged keys. |

## Isolated mode

`COORDINATION_MODE=isolated` provides none of the cross-replica guarantees
above. It exists only to reproduce the over-admission failure.

## Security assumptions

- `SERVER_SECRET` must be identical on all replicas and kept secret. The
  value in `docker-compose.yml` is a development placeholder.
- Client secrets are hard-coded in `server/main.py` for the demo. A real
  deployment would load them from a secret store.
