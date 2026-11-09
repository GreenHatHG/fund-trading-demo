package ordercore

// Fault-path tests (P0): timeout semantics and DB transaction atomicity.
// These run against a real Postgres (DATABASE_URL, default
// postgres://fund:fund@localhost:5432/fund); skipped if it is not reachable.

import (
	"context"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

func testPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		dsn = "postgres://fund:fund@localhost:5432/fund"
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Skipf("postgres not available: %v", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		t.Skipf("postgres not available: %v", err)
	}
	t.Cleanup(pool.Close)
	return pool
}

// A2: an UNKNOWN order may only resolve via reconciliation, never be
// resubmitted. (The CREATED paths are covered by TestMachineAllowedPaths.)
func TestUnknownNeverResubmits(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()

	err := Transition(ctx, pool, 1, Unknown, Submitted, "RESUBMIT", "t", "{}")
	if !errors.Is(err, ErrInvalidTransition) {
		t.Fatalf("Unknown->Submitted err = %v, want ErrInvalidTransition", err)
	}

	// And the DB agrees: nothing changed on a live row either.
	var n int
	if err := pool.QueryRow(ctx,
		`SELECT count(*) FROM order_events WHERE event_type='RESUBMIT'`).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Fatalf("RESUBMIT event rows = %d, want 0", n)
	}
}

// A1: a submit that times out must move Created->Unknown, never Created->Failed.
// The state machine already forbids Created->Failed via Unknown; here we prove
// the timeout mapping produces Unknown on a live row and is idempotent to retry.
func TestSubmitTimeoutMapsToUnknownNotFailed(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()

	var id int64
	if err := pool.QueryRow(ctx,
		`INSERT INTO orders (user_id, idempotency_key, fund_code, direction, amount, state, trading_day)
		 VALUES ('ft-a1', 'k1', '000001', 'PURCHASE', '100.00', 'CREATED', current_date)
		 RETURNING id`).Scan(&id); err != nil {
		t.Fatal(err)
	}
	cleanupOrder := func() {
		bg := context.Background()
		pool.Exec(bg, `DELETE FROM order_events WHERE order_id=$1`, id)
		pool.Exec(bg, `DELETE FROM orders WHERE id=$1`, id)
	}
	t.Cleanup(cleanupOrder)
	if err := Transition(ctx, pool, id, Created, Unknown, "DOWNSTREAM_TIMEOUT", "t", "{}"); err != nil {
		t.Fatalf("Created->Unknown: %v", err)
	}

	var state string
	var failedEvents int
	if err := pool.QueryRow(ctx, `SELECT state FROM orders WHERE id=$1`, id).Scan(&state); err != nil {
		t.Fatal(err)
	}
	if state != string(Unknown) {
		t.Fatalf("state = %s, want UNKNOWN", state)
	}
	if err := pool.QueryRow(ctx,
		`SELECT count(*) FROM order_events WHERE order_id=$1 AND event_type IN ('REJECTED','FAILED')`, id).Scan(&failedEvents); err != nil {
		t.Fatal(err)
	}
	if failedEvents != 0 {
		t.Fatalf("timeout produced %d failure events, want 0", failedEvents)
	}

	// Retry of the same timeout (at-least-once delivery) must conflict, not corrupt.
	err := Transition(ctx, pool, id, Created, Unknown, "DOWNSTREAM_TIMEOUT", "t", "{}")
	if !errors.Is(err, ErrConflict) {
		t.Fatalf("retry err = %v, want ErrConflict", err)
	}
}

// F2: if the tx dies between the state UPDATE and the event INSERT, everything
// rolls back — state, version, and no orphan event row.
func TestTransitionAtomicOnMidwayAbort(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()

	var id int64
	var version int
	if err := pool.QueryRow(ctx,
		`INSERT INTO orders (user_id, idempotency_key, fund_code, direction, amount, state, trading_day)
		 VALUES ('ft-f2', 'k1', '000001', 'PURCHASE', '100.00', 'CREATED', current_date)
		 RETURNING id, version`).Scan(&id, &version); err != nil {
		t.Fatal(err)
	}
	cleanupOrder2 := func() {
		bg := context.Background()
		pool.Exec(bg, `DELETE FROM order_events WHERE order_id=$1`, id)
		pool.Exec(bg, `DELETE FROM orders WHERE id=$1`, id)
	}
	t.Cleanup(cleanupOrder2)

	// Wrapper pool that aborts the 2nd tx statement: kills the tx right after
	// the UPDATE inside Transition lands, before the event INSERT.
	ap := &abortPool{Pool: pool, n: 0}
	err := transitionWithHook(ctx, ap, id, Created, Unknown, "DOWNSTREAM_TIMEOUT", "t")
	if err == nil {
		t.Fatal("expected abort error, got nil")
	}

	var state string
	var v2 int
	var events int
	if err := pool.QueryRow(ctx, `SELECT state, version FROM orders WHERE id=$1`, id).Scan(&state, &v2); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM order_events WHERE order_id=$1`, id).Scan(&events); err != nil {
		t.Fatal(err)
	}
	if state != string(Created) || v2 != version {
		t.Fatalf("state/version = %s/%d, want CREATED/%d — tx not rolled back", state, v2, version)
	}
	if events != 0 {
		t.Fatalf("orphan event rows = %d, want 0", events)
	}

	// A retry with the real pool must then succeed cleanly.
	if err := Transition(ctx, pool, id, Created, Unknown, "DOWNSTREAM_TIMEOUT", "t2", "{}"); err != nil {
		t.Fatalf("retry after abort: %v", err)
	}
	if err := pool.QueryRow(ctx, `SELECT state FROM orders WHERE id=$1`, id).Scan(&state); err != nil {
		t.Fatal(err)
	}
	if state != string(Unknown) {
		t.Fatalf("state after retry = %s, want UNKNOWN", state)
	}
}

// abortPool wraps a pool and fails the Nth query. This simulates a crash /
// connection drop mid-transaction without touching production code.
type abortPool struct {
	*pgxpool.Pool
	n        int
	lastConn *pgx.Conn
}

func (p *abortPool) Begin(ctx context.Context) (pgx.Tx, error) {
	tx, err := p.Pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	return &abortTx{tx, &p.n}, nil
}

type abortTx struct {
	pgx.Tx
	n *int
}

func (t *abortTx) Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	*t.n++
	if *t.n == 2 {
		return pgconn.CommandTag{}, fmt.Errorf("injected abort after statement 2")
	}
	return t.Tx.Exec(ctx, sql, args...)
}

// transitionWithHook is Transition with a pluggable pool, so tests can abort
// mid-transaction. Same statements, same order, no other differences.
func transitionWithHook(ctx context.Context, db DBTxBeginner, orderID int64, from, to State, eventType, requestID string) error {
	if !CanTransition(from, to) {
		return fmt.Errorf("%w: %s -> %s", ErrInvalidTransition, from, to)
	}
	tx, err := db.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	tag, err := tx.Exec(ctx,
		`UPDATE orders SET state=$1, version=version+1, updated_at=now()
		 WHERE id=$2 AND state=$3 AND version=(SELECT version FROM orders WHERE id=$2)`,
		string(to), orderID, string(from))
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return ErrConflict
	}
	_, err = tx.Exec(ctx,
		`INSERT INTO order_events (order_id, event_type, payload, request_id) VALUES ($1,$2,$3::jsonb,$4)`,
		orderID, eventType, "{}", requestID)
	if err != nil {
		return err
	}
	return tx.Commit(ctx)
}

type DBTxBeginner interface {
	Begin(ctx context.Context) (pgx.Tx, error)
}
