// Package ordercore is the single owner of order state transitions.
package ordercore

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type State string

const (
	Created   State = "CREATED"
	Submitted State = "SUBMITTED"
	Confirmed State = "CONFIRMED"
	Failed    State = "FAILED"
	Unknown   State = "UNKNOWN"
)

type Order struct {
	ID             int64
	UserID         string
	IdempotencyKey string
	FundCode       string
	Direction      string
	Amount         string
	State          State
	TradingDay     time.Time
	ConfirmDate    *time.Time
	Version        int
}

var (
	// ErrInvalidTransition means the requested move is not allowed by the machine.
	ErrInvalidTransition = errors.New("ordercore: invalid state transition")
	// ErrConflict means the state changed underneath us (lost race); caller re-reads and retries.
	ErrConflict = errors.New("ordercore: concurrent modification")
)

// allowed maps from-state -> set of to-states. The machine in one table.
var allowed = map[State]map[State]bool{
	// A submit that times out leaves the result unknown even before an explicit
	// SUBMITTED ack — the order may or may not have reached downstream.
	Created:   {Submitted: true, Failed: true, Unknown: true},
	Submitted: {Confirmed: true, Failed: true, Unknown: true},
	Unknown:   {Confirmed: true, Failed: true},
}

func CanTransition(from, to State) bool { return allowed[from][to] }

// Transition moves state with optimistic locking, and appends an event.
// Every state write in the system goes through here. No exceptions.
func Transition(ctx context.Context, db *pgxpool.Pool, orderID int64, from, to State, eventType, requestID string, payload string) error {
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
	if payload == "" {
		payload = "{}"
	}
	_, err = tx.Exec(ctx,
		`INSERT INTO order_events (order_id, event_type, payload, request_id) VALUES ($1,$2,$3::jsonb,$4)`,
		orderID, eventType, payload, requestID)
	if err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// GetByIdempotency implements replay: returns the existing order, ok=false if none.
func GetByIdempotency(ctx context.Context, db *pgxpool.Pool, userID, key string) (*Order, bool, error) {
	var o Order
	err := db.QueryRow(ctx,
		`SELECT id,user_id,idempotency_key,fund_code,direction,amount,state,trading_day,confirm_date,version
		 FROM orders WHERE user_id=$1 AND idempotency_key=$2`, userID, key).
		Scan(&o.ID, &o.UserID, &o.IdempotencyKey, &o.FundCode, &o.Direction,
			&o.Amount, &o.State, &o.TradingDay, &o.ConfirmDate, &o.Version)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	return &o, true, nil
}
