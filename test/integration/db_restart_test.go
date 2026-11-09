package faulttest

// G1: Postgres crash/restart mid-life. The container is really restarted
// (docker compose restart postgres); the test skips itself if docker is not
// available so the suite stays runnable with just psql + the mock.
//
// Verifies after restart:
//   - the data survived (order row readable, whatever state the worker took it to)
//   - replay with the same idempotency key still returns the original order,
//     exactly one row (unique index survived)
//   - new orders can be written (the pool reconnected)

import (
	"context"
	"fmt"
	"os/exec"
	"strings"
	"testing"
	"time"
)

func dockerAvailable(t *testing.T) bool {
	t.Helper()
	if _, err := exec.LookPath("docker"); err != nil {
		return false
	}
	out, err := exec.Command("docker", "compose", "ps", "-q", "postgres").Output()
	return err == nil && strings.TrimSpace(string(out)) != ""
}

func restartPostgres(t *testing.T) {
	t.Helper()
	cmd := exec.Command("docker", "compose", "restart", "postgres")
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("restart postgres: %v: %s", err, out)
	}
	// Wait for healthy, otherwise the first queries after restart fail with
	// "the database system is starting up" instead of a clean reconnect.
	deadline := time.Now().Add(60 * time.Second)
	for time.Now().Before(deadline) {
		out, err := exec.Command("docker", "compose", "ps", "postgres", "--format", "{{.Health}}").Output()
		if err == nil && strings.TrimSpace(string(out)) == "healthy" {
			return
		}
		time.Sleep(500 * time.Millisecond)
	}
	t.Fatal("postgres did not become healthy within 60s")
}

func TestPostgresRestartRecovers(t *testing.T) {
	if !dockerAvailable(t) {
		t.Skip("docker or postgres container not available")
	}

	ctx := context.Background()
	// Unique per run: the DB is shared and old rows would collide with the keys.
	run := fmt.Sprintf("g%d", time.Now().UnixNano())
	user := "ft-" + run

	// Pre-restart: one order through the real api.
	id, _, status := createOrder(t, user, "k1", "000001", "PURCHASE")
	if status != 201 {
		t.Fatalf("create status = %d, want 201", status)
	}
	defer cleanupOrder(t, id)

	// Crash it.
	restartPostgres(t)

	// Data survived the restart: the row is still there, in whatever state
	// the worker had taken it to before the restart.
	waitFor(t, 30*time.Second, "old order readable after restart", func() bool {
		var s string
		return pool.QueryRow(ctx, `SELECT state FROM orders WHERE id=$1`, id).Scan(&s) == nil
	})

	// Idempotency still holds: same key replays the original order.
	gotID, replay, status := createOrder(t, user, "k1", "000001", "PURCHASE")
	if status != 200 || !replay || gotID != id {
		t.Fatalf("replay after restart: status=%d replay=%v id=%d, want 200/true/%d", status, replay, gotID, id)
	}
	var n int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM orders WHERE user_id=$1 AND idempotency_key='k1'`, user).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("rows with key k1 = %d, want 1 (unique index lost?)", n)
	}

	// The pool recovered: a fresh order can be written.
	id2, _, status := createOrder(t, user, "k2", "000001", "REDEEM")
	if status != 201 {
		t.Fatalf("create after restart status = %d, want 201", status)
	}
	defer cleanupOrder(t, id2)
	var s2 string
	if err := pool.QueryRow(ctx, `SELECT state FROM orders WHERE id=$1`, id2).Scan(&s2); err != nil || s2 != "CREATED" {
		t.Fatalf("fresh order after restart: state=%q err=%v, want CREATED", s2, err)
	}
}
