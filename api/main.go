// api — HTTP intake. Validates, enforces idempotency, writes orders as CREATED.
// It never decides final order state.
package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"time"

	"github.com/julienschmidt/httprouter"

	"github.com/jooooody0710/fund-trading-demo/internal/calendar"
	"github.com/jooooody0710/fund-trading-demo/internal/store"
)

func writeJSON(w http.ResponseWriter, code int, v any) { store.WriteJSON(w, code, v) }
func writeErr(w http.ResponseWriter, code int, msg string) { store.WriteErr(w, code, msg) }

func randomID() string {
	var b [8]byte
	_, _ = rand.Read(b[:])
	return hex.EncodeToString(b[:])
}

type createReq struct {
	UserID    string `json:"user_id"`
	FundCode  string `json:"fund_code"`
	Direction string `json:"direction"`
	Amount    string `json:"amount"`
}

type api struct {
	db  *store.DB
	log *slog.Logger
}

func (a *api) handleCreate(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	requestID := r.Header.Get("X-Request-ID")
	if requestID == "" {
		requestID = randomID()
	}
	w.Header().Set("X-Request-ID", requestID)

	idemKey := r.Header.Get("Idempotency-Key")
	var req createReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeErr(w, http.StatusBadRequest, "bad json")
		return
	}
	if req.UserID == "" || req.FundCode == "" || req.Amount == "" ||
		(req.Direction != "PURCHASE" && req.Direction != "REDEEM") {
		writeErr(w, http.StatusBadRequest, "user_id, fund_code, direction(PURCHASE|REDEEM), amount required")
		return
	}
	if idemKey == "" {
		writeErr(w, http.StatusBadRequest, "Idempotency-Key header required")
		return
	}

	// Replay path: same key returns the original order with 200, not 409.
	if existing, ok, err := a.db.GetByIdempotency(r.Context(), req.UserID, idemKey); err != nil {
		writeErr(w, http.StatusInternalServerError, "db error")
		return
	} else if ok {
		writeJSON(w, http.StatusOK, map[string]any{"order": existing, "replay": true})
		return
	}

	tradingDay, confirmDate := calendar.StampAtIntake(time.Now())
	o, err := a.db.InsertOrder(r.Context(), req.UserID, idemKey, req.FundCode, req.Direction, req.Amount, requestID, tradingDay, &confirmDate)
	if errors.Is(err, store.ErrDuplicate) {
		// Lost a race with a duplicate insert — fetch and return the winner.
		existing, ok, _ := a.db.GetByIdempotency(r.Context(), req.UserID, idemKey)
		if ok {
			writeJSON(w, http.StatusOK, map[string]any{"order": existing, "replay": true})
			return
		}
	}
	if err != nil {
		a.log.Error("insert order", "err", err, "request_id", requestID)
		writeErr(w, http.StatusInternalServerError, "db error")
		return
	}
	a.log.Info("order created", "order_id", o.ID, "request_id", requestID)
	writeJSON(w, http.StatusCreated, map[string]any{"order": o, "replay": false})
}

func (a *api) handleGet(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	o, err := a.db.GetOrder(r.Context(), ps.ByName("id"))
	if errors.Is(err, store.ErrNotFound) {
		writeErr(w, http.StatusNotFound, "no such order")
		return
	}
	if err != nil {
		writeErr(w, http.StatusInternalServerError, "db error")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"order": o})
}

func (a *api) handleTrace(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	evs, err := a.db.EventsByRequestID(r.Context(), ps.ByName("requestId"))
	if err != nil {
		writeErr(w, http.StatusInternalServerError, "db error")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"events": evs})
}

func main() {
	log := slog.Default()
	ctx := context.Background()
	pool, err := store.Connect(ctx)
	if err != nil {
		log.Error("connect db", "err", err)
		os.Exit(1)
	}
	a := &api{db: &store.DB{Pool: pool}, log: log}
	r := httprouter.New()
	r.POST("/orders", a.handleCreate)
	r.GET("/orders/:id", a.handleGet)
	r.GET("/debug/trace/:requestId", a.handleTrace)
	log.Info("api listening", "addr", ":8080")
	if err := http.ListenAndServe(":8080", r); err != nil {
		log.Error("listen", "err", err)
		os.Exit(1)
	}
}
