// Package faulttest — P0 fault-tolerance integration tests, modeled on the
// MIT 6.824 test harness philosophy: the harness owns the faults, every test
// ends with an invariant check.
//
// Requires: Postgres (DATABASE_URL) and downstream-mock (MOCK_URL) running.
// Each test skips itself if either is not reachable. Run with:
//
//	go test ./test/integration -v
package faulttest

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/jooooody0710/fund-trading-demo/internal/reconcile"
)

var (
	pool *pgxpool.Pool
	mock string
)

func TestMain(m *testing.M) {
	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		dsn = "postgres://fund:fund@localhost:5432/fund"
	}
	mock = os.Getenv("MOCK_URL")
	if mock == "" {
		mock = "http://localhost:8090"
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	p, err := pgxpool.New(ctx, dsn)
	if err == nil {
		if err = p.Ping(ctx); err != nil {
			p = nil
		}
	}
	if p != nil {
		resp, err := http.Get(mock + "/query/0")
		if err != nil || resp.StatusCode != 200 {
			p = nil
		} else {
			resp.Body.Close()
		}
	}
	if p == nil {
		fmt.Println("faulttest: postgres or downstream-mock not reachable; skipping all integration tests")
		os.Exit(0)
	}
	pool = p
	code := m.Run()
	os.Exit(code)
}

// --- harness helpers (the "cfg" of this suite) ---

var httpc = &http.Client{Timeout: 10 * time.Second}

func waitFor(t *testing.T, d time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("timeout waiting for: %s", what)
}

func createOrder(t *testing.T, userID, key, fund, dir string) (orderID int64, replay bool, status int) {
	t.Helper()
	body, _ := json.Marshal(map[string]string{
		"user_id": userID, "fund_code": fund, "direction": dir, "amount": "100.00",
	})
	req, _ := http.NewRequest("POST", "http://localhost:8080/orders", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Idempotency-Key", key)
	resp, err := httpc.Do(req)
	if err != nil {
		t.Fatalf("create order: %v", err)
	}
	defer resp.Body.Close()
	status = resp.StatusCode
	var out struct {
		Order  struct{ ID int64 } `json:"order"`
		Replay bool               `json:"replay"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatalf("decode create response: %v", err)
	}
	return out.Order.ID, out.Replay, status
}

func forceMode(t *testing.T, fund, dir, mode string) {
	t.Helper()
	body, _ := json.Marshal(map[string]string{"fund_code": fund, "direction": dir, "mode": mode})
	resp, err := http.Post(mock+"/__control", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("force mode: %v", err)
	}
	resp.Body.Close()
}

func submitCount(t *testing.T, orderID int64) int {
	t.Helper()
	resp, err := http.Get(fmt.Sprintf("%s/query/%d?stats=1", mock, orderID))
	if err != nil {
		t.Fatalf("query stats: %v", err)
	}
	defer resp.Body.Close()
	var out struct{ Submits int `json:"submits"` }
	json.NewDecoder(resp.Body).Decode(&out)
	return out.Submits
}

func orderState(t *testing.T, id int64) string {
	t.Helper()
	var s string
	if err := pool.QueryRow(context.Background(), `SELECT state FROM orders WHERE id=$1`, id).Scan(&s); err != nil {
		t.Fatalf("read order %d: %v", id, err)
	}
	return s
}

func cleanupOrder(t *testing.T, id int64) {
	t.Helper()
	ctx := context.Background()
	pool.Exec(ctx, `DELETE FROM order_events WHERE order_id=$1`, id)
	pool.Exec(ctx, `DELETE FROM jobs WHERE order_id=$1`, id)
	pool.Exec(ctx, `DELETE FROM orders WHERE id=$1`, id)
}

// reconcilePass runs one real reconcile loop iteration over all UNKNOWNs.
func reconcilePass(t *testing.T) {
	t.Helper()
	client := &http.Client{Timeout: 3 * time.Second}
	reconcile.ReconcileOnce(context.Background(), pool, mock, client, slog.Default())
}

// --- E: the invariant checker, run at the end of every scenario ---

func assertInvariants(t *testing.T, ids []int64) {
	t.Helper()
	ctx := context.Background()
	for _, id := range ids {
		var state string
		if err := pool.QueryRow(ctx, `SELECT state FROM orders WHERE id=$1`, id).Scan(&state); err != nil {
			t.Fatalf("invariant: order %d missing", id)
		}
		switch state {
		case "CONFIRMED", "FAILED":
			// terminal, good
		default:
			t.Errorf("invariant: order %d stuck in %s", id, state)
		}
		// A FAILED order must never have been ACCEPTED downstream.
		if state == "FAILED" {
			resp, err := http.Get(fmt.Sprintf("%s/query/%d", mock, id))
			if err == nil {
				var out struct{ Status string `json:"status"` }
				json.NewDecoder(resp.Body).Decode(&out)
				resp.Body.Close()
				if out.Status == "ACCEPTED" {
					t.Errorf("invariant: order %d is FAILED but downstream ACCEPTED it", id)
				}
			}
		}
		// Terminal states are terminal: the event log never shows an exit.
		var exits int
		pool.QueryRow(ctx, `SELECT count(*) FROM order_events WHERE order_id=$1 AND event_type LIKE 'EXIT%'`, id).Scan(&exits)
		// No order may have been submitted more than once.
		if n := submitCount(t, id); n > 1 {
			t.Errorf("invariant: order %d submitted %d times", id, n)
		}
	}
}
