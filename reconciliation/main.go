// reconciliation — the safety net. For every UNKNOWN order, query the mock
// downstream: did it ever accept/reject this order? Force a terminal state.
// NEVER resubmits; query only.
package main

import (
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

func main() {
	log := slog.Default()
	ctx := context.Background()
	pool, err := store.Connect(ctx)
	if err != nil {
		log.Error("connect db", "err", err)
		os.Exit(1)
	}
	db := &store.DB{Pool: pool}
	mockURL := os.Getenv("MOCK_URL")
	if mockURL == "" {
		mockURL = "http://localhost:8090"
	}
	client := &http.Client{Timeout: 2 * time.Second}

	tick := time.NewTicker(1 * time.Second)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
			reconcileOnce(ctx, db, mockURL, client, log)
		}
	}
}

func reconcileOnce(ctx context.Context, db *store.DB, mockURL string, client *http.Client, log *slog.Logger) {
	rows, err := db.Pool.Query(ctx, `SELECT id, state FROM orders WHERE state='UNKNOWN' LIMIT 100`)
	if err != nil {
		log.Error("query unknown", "err", err)
		return
	}
	type pair struct {
		id    int64
		state string
	}
	var unknowns []pair
	for rows.Next() {
		var p pair
		if rows.Scan(&p.id, &p.state) == nil {
			unknowns = append(unknowns, p)
		}
	}
	rows.Close()

	for _, p := range unknowns {
		resp, err := client.Get(mockURL + "/query/" + strconv.FormatInt(p.id, 10))
		if err != nil {
			log.Warn("reconcile query failed, will retry next tick", "order_id", p.id, "err", err)
			continue
		}
		if resp.StatusCode != http.StatusOK {
			// Non-200 means the answer is not trustworthy; leave UNKNOWN for the next tick.
			resp.Body.Close()
			log.Warn("reconcile query bad status", "order_id", p.id, "status", resp.StatusCode)
			continue
		}
		var out struct {
			Status string `json:"status"`
			Reason string `json:"reason"`
		}
		decodeErr := json.NewDecoder(resp.Body).Decode(&out)
		resp.Body.Close()
		if decodeErr != nil {
			log.Warn("reconcile query bad body", "order_id", p.id, "err", decodeErr)
			continue
		}
		switch out.Status {
		case "ACCEPTED":
			if err := ordercore.Transition(ctx, db.Pool, p.id, ordercore.Unknown, ordercore.Confirmed, "RECONCILED_ACCEPTED", "reconcile", "{}"); err != nil {
				log.Warn("reconcile transition", "order_id", p.id, "err", err)
			} else {
				log.Info("reconciled -> CONFIRMED", "order_id", p.id)
			}
		case "REJECTED":
			if err := ordercore.Transition(ctx, db.Pool, p.id, ordercore.Unknown, ordercore.Failed, "RECONCILED_REJECTED", "reconcile", "{}"); err != nil {
				log.Warn("reconcile transition", "order_id", p.id, "err", err)
			} else {
				log.Info("reconciled -> FAILED", "order_id", p.id)
			}
		case "UNKNOWN":
			// downstream never saw it; for the demo, an order the registrar never
			// received is safe to fail locally — but ONLY after querying, never by resubmitting.
			if err := ordercore.Transition(ctx, db.Pool, p.id, ordercore.Unknown, ordercore.Failed, "RECONCILED_NEVER_RECEIVED", "reconcile", "{}"); err != nil {
				log.Warn("reconcile transition", "order_id", p.id, "err", err)
			}
		}
	}
}

