// reconciliation — thin loop over internal/reconcile (the safety net).
// NEVER resubmits; query only.
package main

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"time"

	"github.com/jooooody0710/fund-trading-demo/internal/reconcile"
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
			reconcile.ReconcileOnce(ctx, pool, mockURL, client, log)
		}
	}
}



