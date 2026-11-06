# AGENTS.md — notes for LLM coding agents

Read this before doing any work in this repo. `README.md` is for humans; this file is for you.

## What this is

A minimal fund trading backend in Go. Chain: user order → order state machine → downstream submission (mock) → batch confirmation → reconciliation.

Components:

- `api` — HTTP intake on `:8080`. Validates input, enforces idempotency, writes orders as `CREATED`. It never decides final state.
- `order-core` — single owner of order state transitions. All state writes go through it.
- `downstream-mock` — fake fund registrar on `:8090`. Returns success, explicit failure, or timeout. A timeout means UNKNOWN, not failure.
- `job-worker` — batch jobs: submit pending orders, confirm T+N, reconcile UNKNOWN orders.
- `reconciliation` — queries the downstream for UNKNOWN orders and forces a terminal state. It is a safety net; it never resubmits.
- `internal/store` — Postgres connection + embedded `schema.sql` (applied at startup, idempotent).
- `internal/calendar` — T+N trading-day math.

## How to run

```bash
# 1. Start Postgres (must be up before the Go services)
docker compose up -d

# 2. Wait for it to be healthy, then start all four services
docker compose ps            # postgres should show "healthy"
make run                     # starts downstream-mock, api, job-worker, reconciliation
```

Notes for agents:

- `make run` backgrounds all four services with `&`. Run it in a shell you can clean up; `pkill -f 'go run'` stops them.
- DB default DSN is `postgres://fund:fund@localhost:5432/fund`; override with `DATABASE_URL`. Ports: api `:8080`, downstream-mock `:8090`.
- No manual migration step — each service applies `internal/store/schema.sql` on connect.
- Prefer `go run ./api` etc. individually when iterating on one service; `make run` is for the full demo.

## Verify it works

```bash
go build ./... && go test ./...

curl -s -X POST localhost:8080/orders \
  -H 'Content-Type: application/json' -H 'Idempotency-Key: k1' \
  -d '{"user_id":"u1","fund_code":"000001","direction":"PURCHASE","amount":"100.00"}'
# replay with the same key returns the original order with 200 and "replay": true

curl -s localhost:8080/orders/<order_id>
curl -s 'localhost:8080/debug/trace/<request_id>'

# optional: force downstream behavior for tests
curl -s -X POST localhost:8090/__control -d '{"fund_code":"000001","direction":"PURCHASE","mode":"timeout"}'
```

## Hard constraints — do not violate

- NEVER write order state outside `order-core`.
- NEVER treat a downstream timeout as failure; it maps to `UNKNOWN`. `UNKNOWN` resolves only via reconciliation (query, never resubmit).
- NEVER rely on application-level dedup alone; the unique index on `(user_id, idempotency_key)` must exist.
- Idempotent replay returns the original order with `200`, not `409`.

## Code style

- Keep changes scoped to the task. No speculative abstractions, no drive-by refactors.
- Comments only on lines with real traps ("why", not "what").
- Tests: behavior only, P0 paths only (data loss, state corruption, crash).
- Elixir : keep nesting shallow. Prefer `with` + small private helpers over `if`/`case` inside closures; one level of abstraction per function. No `|> case do` chained onto a multi-level block. Avoid long positional arg lists where a keyword list or pattern-matched function heads read better. Run `mix compile --warnings-as-errors` before claiming done.
