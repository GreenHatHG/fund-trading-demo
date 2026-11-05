# Fund Trading Demo

A minimal fund trading backend: user order → order state machine → downstream submission (mock) → batch confirmation → reconciliation.

## Quick start

```bash
docker compose up -d          # Postgres
make run                      # api (:8080), downstream-mock (:8090), job-worker, reconciliation
```

Then:

```bash
curl -s -X POST localhost:8080/orders \
  -H 'Content-Type: application/json' -H 'Idempotency-Key: k1' \
  -d '{"user_id":"u1","fund_code":"000001","direction":"PURCHASE","amount":"100.00"}'
```

See `AGENTS.md` for agent-facing docs and `docs/en/20261104-overview.md` for the design overview.
