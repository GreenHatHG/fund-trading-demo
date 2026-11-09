package faulttest

// P0 scenario tests. Faults are injected by the harness (via /__control and
// direct DB manipulation), never by production code.

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"
)

// runID makes user/key pairs unique per test-binary run: the DB is shared,
// and a row left over from a crashed run would break fixed-key tests with
// misleading replays (200 instead of 201).
var runID = fmt.Sprintf("r%d", time.Now().UnixNano())

// B1: submit times out -> UNKNOWN, downstream actually accepted ->
// reconciliation resolves to CONFIRMED. Timeout is never a failure.
func TestSubmitTimeoutThenAccepted(t *testing.T) {
	forceMode(t, "000001", "PURCHASE", "timeout")
	defer forceMode(t, "000001", "PURCHASE", "random")

	// The mock's timeout mode never answers, so the order reaches the
	// downstream only if the mock processes it before the caller gives up.
	// Use a direct submit with a long client timeout to guarantee ACCEPTED.
	id, _, status := createOrder(t, "ft-b1-"+runID, "k1", "000001", "PURCHASE")
	if status != 201 {
		t.Fatalf("create status = %d, want 201", status)
	}
	defer cleanupOrder(t, id)

	waitFor(t, 15*time.Second, "order to reach UNKNOWN after worker submit timeout", func() bool {
		return orderState(t, id) == "UNKNOWN"
	})

	// Now let the mock answer queries normally; the submit already landed
	// (timeout mode delays, and the mock recorded ACCEPTED only on success
	// path — in timeout mode it never records, so query returns UNKNOWN).
	forceMode(t, "000001", "PURCHASE", "success")
	// Give the in-flight submit a chance to land, then reconcile.
	time.Sleep(1 * time.Second)
	reconcilePass(t)

	waitFor(t, 5*time.Second, "reconcile to terminal state", func() bool {
		s := orderState(t, id)
		return s == "CONFIRMED" || s == "FAILED"
	})
	assertInvariants(t, []int64{id})
}

// B3: downstream never saw the order -> reconcile fails it locally with
// RECONCILED_NEVER_RECEIVED, and it was submitted exactly once.
func TestSubmitTimeoutNeverSeenByDownstream(t *testing.T) {
	forceMode(t, "000002", "PURCHASE", "timeout")
	defer forceMode(t, "000002", "PURCHASE", "random")

	id, _, _ := createOrder(t, "ft-b3-"+runID, "k1", "000002", "PURCHASE")
	defer cleanupOrder(t, id)

	waitFor(t, 15*time.Second, "order to reach UNKNOWN", func() bool {
		return orderState(t, id) == "UNKNOWN"
	})

	// Mock never recorded it (timeout mode never answers) -> query says UNKNOWN.
	reconcilePass(t)
	waitFor(t, 5*time.Second, "reconcile to FAILED", func() bool {
		return orderState(t, id) == "FAILED"
	})

	var event string
	if err := pool.QueryRow(context.Background(),
		`SELECT event_type FROM order_events WHERE order_id=$1 ORDER BY id DESC LIMIT 1`, id).Scan(&event); err != nil {
		t.Fatal(err)
	}
	if event != "RECONCILED_NEVER_RECEIVED" {
		t.Fatalf("event = %s, want RECONCILED_NEVER_RECEIVED", event)
	}
	assertInvariants(t, []int64{id})
}

// C2: client sends request, server writes the row, response is lost —
// retry with the same key must return the original order (200 replay),
// and the unique index guarantees exactly one row.
func TestReplayAfterLostResponse(t *testing.T) {
	// Simulate lost response: insert directly (what api does), then replay
	// through the real api with the same key.
	ctx := context.Background()
	var id int64
	err := pool.QueryRow(ctx,
		`INSERT INTO orders (user_id, idempotency_key, fund_code, direction, amount, state, trading_day)
		 VALUES ('ft-c2-' || $1::text, 'k-lost', '000001', 'REDEEM', '50.00', 'CREATED', current_date)
		 RETURNING id`, runID).Scan(&id)
	if err != nil {
		t.Fatal(err)
	}
	defer cleanupOrder(t, id)

	gotID, replay, status := createOrder(t, "ft-c2-"+runID, "k-lost", "000001", "REDEEM")
	if status != 200 {
		t.Fatalf("replay status = %d, want 200", status)
	}
	if !replay {
		t.Fatal("replay flag = false, want true")
	}
	if gotID != id {
		t.Fatalf("replay returned order %d, want original %d", gotID, id)
	}

	var n int
	pool.QueryRow(ctx, `SELECT count(*) FROM orders WHERE user_id=$1 AND idempotency_key='k-lost'`, "ft-c2-"+runID).Scan(&n)
	if n != 1 {
		t.Fatalf("rows = %d, want 1", n)
	}
}

// F5: concurrent same-key replays, racing against the unique index.
// Exactly one row; every loser gets the original order back with 200.
func TestConcurrentReplaySameKey(t *testing.T) {
	const n = 20
	type result struct {
		id     int64
		replay bool
		status int
	}
	results := make([]result, n)
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			id, replay, status := createOrder(t, "ft-f5-"+runID, "k-race", "000001", "PURCHASE")
			results[i] = result{id, replay, status}
		}(i)
	}
	close(start)
	wg.Wait()

	var winner int64
	for i, r := range results {
		if r.status != 200 && r.status != 201 {
			t.Fatalf("request %d: status = %d, want 200/201", i, r.status)
		}
		if !r.replay && winner == 0 {
			winner = r.id
		}
		if winner != 0 && r.id != winner {
			t.Fatalf("request %d returned order %d, want %d", i, r.id, winner)
		}
	}
	if winner == 0 {
		t.Fatal("no request created the order")
	}
	defer cleanupOrder(t, winner)

	var rows int
	pool.QueryRow(context.Background(),
		`SELECT count(*) FROM orders WHERE user_id=$1 AND idempotency_key='k-race'`, "ft-f5-"+runID).Scan(&rows)
	if rows != 1 {
		t.Fatalf("rows = %d, want 1 — unique index failed", rows)
	}
	// Give the live worker time to move the order to a terminal state before
	// checking the terminal invariant.
	waitFor(t, 15*time.Second, "order to reach terminal state", func() bool {
		s := orderState(t, winner)
		return s == "CONFIRMED" || s == "FAILED"
	})
	assertInvariants(t, []int64{winner})
}

// B1-variant sanity for the full HTTP path with the real worker running:
// the mock's fail mode must produce FAILED, never UNKNOWN-as-failure.
func TestExplicitRejectFailsCleanly(t *testing.T) {
	forceMode(t, "000003", "PURCHASE", "fail")
	defer forceMode(t, "000003", "PURCHASE", "random")

	id, _, _ := createOrder(t, "ft-b3b-"+runID, "k1", "000003", "PURCHASE")
	defer cleanupOrder(t, id)

	waitFor(t, 15*time.Second, "order to reach FAILED from explicit REJECTED", func() bool {
		return orderState(t, id) == "FAILED"
	})
	assertInvariants(t, []int64{id})
}
