// DB wraps pgxpool with the queries used by the Go services.
package store

import (
	"context"
	"errors"
	"math/rand"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

var (
	ErrNotFound  = errors.New("store: not found")
	ErrDuplicate = errors.New("store: duplicate")
)

type DB struct {
	Pool *pgxpool.Pool
}

type Order struct {
	ID             int64      `json:"id"`
	UserID         string     `json:"user_id"`
	IdempotencyKey string     `json:"idempotency_key"`
	FundCode       string     `json:"fund_code"`
	Direction      string     `json:"direction"`
	Amount         string     `json:"amount"`
	State          string     `json:"state"`
	TradingDay     time.Time  `json:"trading_day"`
	ConfirmDate    *time.Time `json:"confirm_date"`
	Version        int        `json:"version"`
}

const orderCols = `id,user_id,idempotency_key,fund_code,direction,amount,state,trading_day,confirm_date,version`

func scanOrder(row pgx.Row) (*Order, error) {
	var o Order
	err := row.Scan(&o.ID, &o.UserID, &o.IdempotencyKey, &o.FundCode, &o.Direction,
		&o.Amount, &o.State, &o.TradingDay, &o.ConfirmDate, &o.Version)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	return &o, nil
}

func (d *DB) GetByIdempotency(ctx context.Context, userID, key string) (*Order, bool, error) {
	o, err := scanOrder(d.Pool.QueryRow(ctx,
		`SELECT `+orderCols+` FROM orders WHERE user_id=$1 AND idempotency_key=$2`, userID, key))
	if errors.Is(err, ErrNotFound) {
		return nil, false, nil
	}
	return o, err == nil, err
}

func (d *DB) GetOrder(ctx context.Context, id string) (*Order, error) {
	return scanOrder(d.Pool.QueryRow(ctx,
		`SELECT `+orderCols+` FROM orders WHERE id=$1::bigint`, id))
}

// InsertOrder writes a CREATED order. Unique index on (user_id, idempotency_key)
// is the real dedup; ErrDuplicate maps pg's 23505.
func (d *DB) InsertOrder(ctx context.Context, userID, idemKey, fundCode, direction, amount, requestID string, tradingDay time.Time, confirmDate *time.Time) (*Order, error) {
	// One transaction: order row and its event must live or die together.
	tx, err := d.Pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)

	var o Order
	err = tx.QueryRow(ctx,
		`INSERT INTO orders (user_id,idempotency_key,fund_code,direction,amount,state,trading_day,confirm_date)
		 VALUES ($1,$2,$3,$4,$5,'CREATED',$6,$7)
		 ON CONFLICT (user_id, idempotency_key) DO NOTHING
		 RETURNING `+orderCols,
		userID, idemKey, fundCode, direction, amount, tradingDay, confirmDate).
		Scan(&o.ID, &o.UserID, &o.IdempotencyKey, &o.FundCode, &o.Direction, &o.Amount, &o.State, &o.TradingDay, &o.ConfirmDate, &o.Version)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrDuplicate
	}
	if err != nil {
		return nil, err
	}
	_, err = tx.Exec(ctx,
		`INSERT INTO order_events (order_id, event_type, payload, request_id) VALUES ($1,'ORDER_CREATED','{}',$2)`, o.ID, requestID)
	if err != nil {
		return nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	return &o, nil
}

type Event struct {
	ID        int64     `json:"id"`
	OrderID   int64     `json:"order_id"`
	EventType string    `json:"event_type"`
	RequestID string    `json:"request_id"`
	CreatedAt time.Time `json:"created_at"`
}

func (d *DB) EventsByRequestID(ctx context.Context, requestID string) ([]Event, error) {
	rows, err := d.Pool.Query(ctx,
		`SELECT id,order_id,event_type,request_id,created_at FROM order_events WHERE request_id=$1 ORDER BY id`, requestID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var evs []Event
	for rows.Next() {
		var e Event
		if err := rows.Scan(&e.ID, &e.OrderID, &e.EventType, &e.RequestID, &e.CreatedAt); err != nil {
			return nil, err
		}
		evs = append(evs, e)
	}
	return evs, rows.Err()
}

// EnqueueJob inserts a job unless an active one already exists for the order (unique partial index).
func (d *DB) EnqueueJob(ctx context.Context, orderID int64, jobType string) error {
	_, err := d.Pool.Exec(ctx,
		`INSERT INTO jobs (order_id, job_type, state) VALUES ($1,$2,'PENDING')
		 ON CONFLICT (order_id) WHERE state IN ('PENDING','RUNNING') DO NOTHING`, orderID, jobType)
	return err
}

// ClaimNextJob picks one due PENDING job and flips it to RUNNING, atomically (FOR UPDATE SKIP LOCKED).
func (d *DB) ClaimNextJob(ctx context.Context) (id, orderID int64, jobType string, attempts, maxAttempts int, err error) {
	err = d.Pool.QueryRow(ctx,
		`UPDATE jobs SET state='RUNNING', attempts=attempts+1, updated_at=now()
		 WHERE id = (SELECT id FROM jobs WHERE state='PENDING' AND run_after<=now() ORDER BY id FOR UPDATE SKIP LOCKED LIMIT 1)
		 RETURNING id, order_id, job_type, attempts, max_attempts`).
		Scan(&id, &orderID, &jobType, &attempts, &maxAttempts)
	if errors.Is(err, pgx.ErrNoRows) {
		err = nil
	}
	return
}

func (d *DB) FinishJob(ctx context.Context, id int64, dead bool) error {
	state := "DONE"
	if dead {
		state = "DEAD"
	}
	_, err := d.Pool.Exec(ctx, `UPDATE jobs SET state=$1, updated_at=now() WHERE id=$2`, state, id)
	return err
}

// RetryJob puts a job back to PENDING with exponential backoff, or marks it DEAD past max attempts.
func (d *DB) RetryJob(ctx context.Context, id int64, attempts, maxAttempts int) error {
	if attempts >= maxAttempts {
		_, err := d.Pool.Exec(ctx, `UPDATE jobs SET state='DEAD', updated_at=now() WHERE id=$1`, id)
		return err
	}
	backoff := time.Duration(1<<uint(attempts)) * time.Second
	_, err := d.Pool.Exec(ctx,
		`UPDATE jobs SET state='PENDING', run_after=now()+make_interval(secs=>$1), updated_at=now() WHERE id=$2`,
		backoff.Seconds(), id)
	return err
}

// StaleSubmittedOrders lists SUBMITTED orders with no downstream answer recently.
// It only READS; the state write stays in ordercore.Transition (hard constraint).
func (d *DB) StaleSubmittedOrders(ctx context.Context, olderThan time.Duration, limit int) ([]int64, error) {
	rows, err := d.Pool.Query(ctx,
		`SELECT id FROM orders WHERE state='SUBMITTED' AND updated_at < now()-make_interval(secs=>$1) LIMIT $2`, olderThan.Seconds(), limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var ids []int64
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	return ids, rows.Err()
}

var _ = rand.Int // silence unused in some build configs
