// job-worker — batch tasks: submit CREATED orders downstream, mark stale
// SUBMITTED orders as UNKNOWN. Reconciliation is the separate safety net.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"os"
	"strconv"
	"time"

	"github.com/jooooody0710/fund-trading-demo/internal/ordercore"
	"github.com/jooooody0710/fund-trading-demo/internal/store"
)

type worker struct {
	db       *store.DB
	mockURL  string
	client   *http.Client
	log      *slog.Logger
}

func (wk *worker) handleSubmitJob(ctx context.Context, orderID int64, requestID string) error {
	o, err := wk.db.GetOrder(ctx, itoa(orderID))
	if err != nil {
		return err
	}
	// Idempotent handler: only CREATED orders move. A re-delivered job on a
	// SUBMITTED/terminal order is a no-op, not an error.
	if o.State != string(ordercore.Created) {
		wk.log.Info("submit job: order not CREATED, skipping", "order_id", orderID, "state", o.State)
		return nil
	}
	payload, _ := json.Marshal(map[string]any{
		"order_id": o.ID, "fund_code": o.FundCode, "direction": o.Direction,
		"amount": o.Amount, "request_id": requestID,
	})
	// Enforce the timeout contract locally: if the mock stalls, we give up and mark UNKNOWN.
	cctx, cancel := context.WithTimeout(ctx, 800*time.Millisecond)
	defer cancel()
	req, err := http.NewRequestWithContext(cctx, "POST", wk.mockURL+"/submit", bytes.NewReader(payload))
	if err != nil {
		return wk.transition(ctx, orderID, ordercore.Created, ordercore.Unknown, "DOWNSTREAM_TIMEOUT", requestID)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := wk.client.Do(req)
	if err != nil {
		// Timeout or transport error: result UNKNOWN, never FAILED.
		wk.log.Warn("downstream timeout, order -> UNKNOWN", "order_id", orderID, "err", err)
		return wk.transition(ctx, orderID, ordercore.Created, ordercore.Unknown, "DOWNSTREAM_TIMEOUT", requestID)
	}
	defer resp.Body.Close()
	var out struct {
		Status string `json:"status"`
		Reason string `json:"reason"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return err
	}
	switch out.Status {
	case "ACCEPTED":
		return wk.transition(ctx, orderID, ordercore.Created, ordercore.Submitted, "SUBMITTED", requestID)
	case "REJECTED":
		return wk.transition(ctx, orderID, ordercore.Created, ordercore.Failed, "REJECTED:"+out.Reason, requestID)
	default:
		return wk.transition(ctx, orderID, ordercore.Created, ordercore.Unknown, "UNEXPECTED_RESPONSE", requestID)
	}
}

func (wk *worker) transition(ctx context.Context, orderID int64, from, to ordercore.State, eventType, requestID string) error {
	err := ordercore.Transition(ctx, wk.db.Pool, orderID, from, to, eventType, requestID, "{}")
	if err == nil && to == ordercore.Submitted {
		// T+N confirm happens later; enqueue a RECONCILE job as the safety net.
		_ = wk.db.EnqueueJob(ctx, orderID, "RECONCILE")
	}
	return err
}

func (wk *worker) runOnce(ctx context.Context) {
	// Sweep first so an idle queue still advances stale SUBMITTED orders.
	wk.sweepStaleSubmits(ctx)
	for i := 0; i < 10; i++ {
		id, orderID, jobType, attempts, maxAttempts, err := wk.db.ClaimNextJob(ctx)
		if err != nil || id == 0 {
			return
		}
		switch jobType {
		case "SUBMIT":
			if err := wk.handleSubmitJob(ctx, orderID, jobRequestID(orderID)); err != nil {
				if err := wk.db.RetryJob(ctx, id, attempts, maxAttempts); err != nil {
					wk.log.Error("retry job", "err", err)
				}
			} else if err := wk.db.FinishJob(ctx, id, false); err != nil {
				wk.log.Error("finish job", "err", err)
			}
		case "RECONCILE":
			// handled by reconciliation component; here we just finish stale ones
			_ = wk.db.FinishJob(ctx, id, false)
		}
	}
}

func (wk *worker) sweepStaleSubmits(ctx context.Context) {
	ids, err := wk.db.StaleSubmittedOrders(ctx, 5*time.Second, 100)
	if err != nil {
		wk.log.Error("claim timeout orders", "err", err)
		return
	}
	for _, id := range ids {
		// Submitted -> Unknown is legal per the machine; sweep via order-core rule.
		if err := ordercore.Transition(ctx, wk.db.Pool, id, ordercore.Submitted, ordercore.Unknown, "TIMEOUT_SWEEP", "sweep", "{}"); err != nil {
			wk.log.Warn("sweep transition", "order_id", id, "err", err)
		}
	}
}

func jobRequestID(orderID int64) string { return "job-submit-" + itoa(orderID) }

func itoa(n int64) string { return strconv.FormatInt(n, 10) }

func main() {
	log := slog.Default()
	ctx := context.Background()
	pool, err := store.Connect(ctx)
	if err != nil {
		log.Error("connect db", "err", err)
		os.Exit(1)
	}
	mockURL := os.Getenv("MOCK_URL")
	if mockURL == "" {
		mockURL = "http://localhost:8090"
	}
	// Enqueue SUBMIT jobs for any CREATED orders (at-least-once: safe to re-run).
	db := &store.DB{Pool: pool}
	rows, err := pool.Query(ctx, `SELECT id FROM orders WHERE state='CREATED'`)
	if err == nil {
		for rows.Next() {
			var id int64
			if rows.Scan(&id) == nil {
				_ = db.EnqueueJob(ctx, id, "SUBMIT")
			}
		}
		rows.Close()
	}

	wk := &worker{db: db, mockURL: mockURL, client: &http.Client{}, log: log}
	tick := time.NewTicker(200 * time.Millisecond)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
			wk.runOnce(ctx)
		}
	}
}
