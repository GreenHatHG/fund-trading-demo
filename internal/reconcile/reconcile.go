// Package reconcile is the safety net. For every UNKNOWN order, query the mock
// downstream: did it ever accept/reject this order? Force a terminal state.
// NEVER resubmits; query only.
package reconcile

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"strconv"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/jooooody0710/fund-trading-demo/internal/ordercore"
)

func ReconcileOnce(ctx context.Context, pool *pgxpool.Pool, mockURL string, client *http.Client, log *slog.Logger) {
	rows, err := pool.Query(ctx, `SELECT id, state FROM orders WHERE state='UNKNOWN' LIMIT 100`)
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
			if err := ordercore.Transition(ctx, pool, p.id, ordercore.Unknown, ordercore.Confirmed, "RECONCILED_ACCEPTED", "reconcile", "{}"); err != nil {
				log.Warn("reconcile transition", "order_id", p.id, "err", err)
			} else {
				log.Info("reconciled -> CONFIRMED", "order_id", p.id)
			}
		case "REJECTED":
			if err := ordercore.Transition(ctx, pool, p.id, ordercore.Unknown, ordercore.Failed, "RECONCILED_REJECTED", "reconcile", "{}"); err != nil {
				log.Warn("reconcile transition", "order_id", p.id, "err", err)
			} else {
				log.Info("reconciled -> FAILED", "order_id", p.id)
			}
		case "UNKNOWN":
			// downstream never saw it; for the demo, an order the registrar never
			// received is safe to fail locally — but ONLY after querying, never by resubmitting.
			if err := ordercore.Transition(ctx, pool, p.id, ordercore.Unknown, ordercore.Failed, "RECONCILED_NEVER_RECEIVED", "reconcile", "{}"); err != nil {
				log.Warn("reconcile transition", "order_id", p.id, "err", err)
			}
		}
	}
}
