---
created_at: 2026-11-05
updated_at: 2026-11-05
topic: insertorder-transaction-atomicity
status: resolved
affected_modules: [internal/store/db.go]
tags: [postgres, transaction, atomicity, idempotency, pgx]
---

# InsertOrder: Two Inserts Must Be Atomic

## 1. Context & Problem
`InsertOrder` in `internal/store/db.go` runs two SQL statements: insert into `orders`, then insert into `order_events` (`ORDER_CREATED`). They ran on `d.Pool` with no transaction. If the DB died between them, the order row existed but the event row was lost — broken audit trail.

## 2. Pitfalls & Failed Attempts (Gotchas - DO NOT REPEAT)
- **Attempt**: Keep two separate `Pool.Exec`/`Pool.QueryRow` calls.
  - **Failure Reason**: No atomicity. Crash between statement 1 and 2 leaves an orphan order with no event.
- **Gotcha**: On the duplicate path (`ON CONFLICT DO NOTHING` returns `pgx.ErrNoRows`), the function returns early. Without `defer tx.Rollback(ctx)`, that would leak the open transaction.

## 3. Decision & Verified Solution
- **Chosen Approach**: Wrap both inserts in one pgx transaction (`Begin` → both statements on `tx` → `Commit`). `defer tx.Rollback(ctx)` covers early returns; `Commit` makes both rows visible atomically. On retry after a crash, the unique index on `(user_id, idempotency_key)` keeps idempotency.
- **Key Changes**: `internal/store/db.go` `InsertOrder` only.
- **Minimal Code / Command Reference**:
```go
tx, err := d.Pool.Begin(ctx)
if err != nil { return nil, err }
defer tx.Rollback(ctx) // early returns (ErrDuplicate, errors) roll back

err = tx.QueryRow(ctx, `INSERT INTO orders ... RETURNING `+orderCols, ...).Scan(...)
if errors.Is(err, pgx.ErrNoRows) { return nil, ErrDuplicate }
_, err = tx.Exec(ctx, `INSERT INTO order_events ...`, o.ID, requestID)
if err != nil { return nil, err }
if err := tx.Commit(ctx); err != nil { return nil, err }
return &o, nil
```

## 4. Hard Constraints for Future AI
- NEVER run two related writes (order + event) on `d.Pool` directly — ALWAYS use one transaction.
- ALWAYS pair `Begin` with `defer tx.Rollback(ctx)` so error/duplicate paths cannot leak the transaction.
- Dedup still relies ONLY on the unique index `(user_id, idempotency_key)`; do not add app-level checks.
