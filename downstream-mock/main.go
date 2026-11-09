// downstream-mock — simulates the fund registrar.
// Outcomes per order, controlled via /__control: success, failure, or timeout.
// A timeout here means the caller's result is UNKNOWN, never failure.
package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/julienschmidt/httprouter"
)

type mode string

const (
	modeSuccess mode = "success"
	modeFail    mode = "fail"
	modeTimeout mode = "timeout"
)

type mock struct {
	mu      sync.Mutex
	modes   map[string]mode
	results map[string]map[string]any
	submits map[string]int // order_id -> count of completed submissions (test-only)
}

// decide picks the outcome: explicit mode if set, else 70% success / 20% fail / 10% timeout.
func (m *mock) decide(key string) mode {
	m.mu.Lock()
	defer m.mu.Unlock()
	if mo, ok := m.modes[key]; ok {
		return mo
	}
	switch r := time.Now().UnixNano() % 10; {
	case r < 7:
		return modeSuccess
	case r < 9:
		return modeFail
	default:
		return modeTimeout
	}
}

type submitReq struct {
	OrderID   int64  `json:"order_id"`
	FundCode  string `json:"fund_code"`
	Direction string `json:"direction"`
	Amount    string `json:"amount"`
	RequestID string `json:"request_id"`
}

func (m *mock) handleSubmit(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	var req submitReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "bad json", 400)
		return
	}
	key := req.FundCode + "|" + req.Direction
	mo := m.decide(key)

	// Simulated processing latency so the worker's timeouts are real.
	latency := 50 * time.Millisecond
	if mo == modeTimeout {
		latency = 2 * time.Second
	}
	select {
	case <-time.After(latency):
	case <-r.Context().Done():
		return // caller gave up: this is the timeout path
	}

	m.mu.Lock()
	m.submits[idKey(req.OrderID)]++
	m.mu.Unlock()

	switch mo {
	case modeSuccess:
		res := map[string]any{"status": "ACCEPTED", "order_id": req.OrderID}
		m.record(idKey(req.OrderID), res)
		writeJSON(w, 200, res)
	case modeFail:
		res := map[string]any{"status": "REJECTED", "reason": "insufficient balance", "order_id": req.OrderID}
		m.record(idKey(req.OrderID), res)
		writeJSON(w, 200, res)
	case modeTimeout:
		// never answers; caller times out into UNKNOWN
	}
}

func idKey(orderID int64) string { return fmt.Sprint(orderID) }

// handleControl lets tests force an outcome: POST /__control {"fund_code":"000001","direction":"PURCHASE","mode":"timeout"}
func (m *mock) handleControl(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	var body struct {
		FundCode  string `json:"fund_code"`
		Direction string `json:"direction"`
		Mode      string `json:"mode"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil || body.Mode == "" {
		http.Error(w, "need fund_code, direction, mode(success|fail|timeout|random)", 400)
		return
	}
	m.mu.Lock()
	if body.Mode == "random" {
		delete(m.modes, body.FundCode+"|"+body.Direction)
	} else {
		m.modes[body.FundCode+"|"+body.Direction] = mode(body.Mode)
	}
	m.mu.Unlock()
	writeJSON(w, 200, map[string]string{"ok": "true"})
}

// handleQuery lets reconciliation ask: did you ever see this order? Returns terminal result or "UNKNOWN".
func (m *mock) handleQuery(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	id := ps.ByName("orderId")
	m.mu.Lock()
	res, ok := m.results[id]
	submits := m.submits[id]
	m.mu.Unlock()
	if r.URL.Query().Get("stats") == "1" {
		writeJSON(w, 200, map[string]any{"submits": submits})
		return
	}
	if !ok {
		writeJSON(w, 200, map[string]any{"status": "UNKNOWN"})
		return
	}
	writeJSON(w, 200, res)
}

func (m *mock) record(id string, res map[string]any) {
	m.mu.Lock()
	m.results[id] = res
	m.mu.Unlock()
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func main() {
	m := &mock{modes: map[string]mode{}, results: map[string]map[string]any{}, submits: map[string]int{}}
	r := httprouter.New()
	r.POST("/submit", m.handleSubmit)
	r.POST("/__control", m.handleControl)
	r.GET("/query/:orderId", m.handleQuery)
	addr := ":8090"
	if p := os.Getenv("MOCK_ADDR"); p != "" {
		addr = p
	}
	log.Println("downstream-mock listening on", addr)
	log.Fatal(http.ListenAndServe(addr, r))
}
