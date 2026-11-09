# Load tests

k6 scripts for the api intake. Each script runs its own DB validation at the end
(`k6/x/sql`): it checks exactly the rows that run wrote, scoped by the round's
idempotency-key prefix, and prints `VERIFY: PASS` / `FAIL` in the summary.
A FAIL does not fail the k6 exit code (thresholds do) — read the summary tail.

## Round 1: intake

```bash
docker compose up -d && make run          # services up
docker compose exec -T postgres psql -U fund -d fund -c \
  'TRUNCATE orders, order_events, jobs RESTART IDENTITY'  # clean slate (optional)
k6 run loadtest/intake.js                 # 50 req/s via RATES
```

Env knobs: `BASE_URL`, `RATES`, `DURATION` per stage, `ONLY=normal|replay|bad`,
`RUN` (round id; auto-generated from timestamp when unset).

Scenarios:
- normal: unique user + idempotency key per request, expect `201 replay:false`,
  plus round-trip checks (response echoes user_id/amount/direction/key)
- replay: fixed key per VU, expect `200 replay:true` after first call
- bad: missing fields / bad direction, expect `400`

Invariants checked post-run (scoped to `k<RUN>%` keys):
1. No duplicate `(user_id, idempotency_key)` rows (unique index holds)
2. No rows with amount<=0 or invalid direction
3. Round-trip: stored amount/fund_code match what the script sends
4. Every `CREATED` order older than 120s has a SUBMIT job (younger = pre-enqueue backlog)
5. Every order has exactly one `ORDER_CREATED` event
6. No `UNKNOWN` order older than 15s (reconciliation must drain it)

## Round 2: downstream timeout flow

```bash
k6 run loadtest/timeout-flow.js      # forces mode=timeout for fund 000002 PURCHASE
```

At the end of the run the script polls the DB (`DRAIN_WAIT`, default 60s) until
every order of this round is terminal, then checks:
- nothing left in CREATED/SUBMITTED/UNKNOWN for this round
- at least one `RECONCILED_NEVER_RECEIVED` event (a 0-reconcile run fails — no vacuous pass)
- no order has more than one `RECONCILED_*` event

Validates: DOWNSTREAM_TIMEOUT -> UNKNOWN -> reconciliation (query only) ->
terminal state. Timed-out orders were never recorded by the mock, so
reconciliation resolves them as `RECONCILED_NEVER_RECEIVED` -> FAILED.

## Notes

- job-worker batch is 10 / 200ms tick → ~50 orders/s intake ceiling for submit;
  CREATED backlog at higher rates is expected, check `jobs` table instead of panicking.
  Invariant 4 only flags CREATED orders older than 120s for this reason.
- Downstream outcomes (hence FAILED/CONFIRMED ratio) follow the mock's control
  mode; round 1 keeps it on `random`.
- `lib/db.js` holds the shared DB helpers (`k6/x/sql`, postgres driver, built into k6 v1.4).

## Thresholds (k6 exits non-zero on breach)

- p95 < 100ms, p99 < 300ms
- http_req_failed < 1%
- checks pass rate > 99%
