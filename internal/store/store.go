// Package store owns the DB connection and schema setup.
package store

import (
	"context"
	_ "embed"
	"fmt"
	"os"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

//go:embed schema.sql
var schemaSQL string

func DSN() string {
	if d := os.Getenv("DATABASE_URL"); d != "" {
		return d
	}
	return "postgres://fund:fund@localhost:5432/fund"
}

func Connect(ctx context.Context) (*pgxpool.Pool, error) {
	pool, err := pgxpool.New(ctx, DSN())
	if err != nil {
		return nil, err
	}
	// Schema may be applied concurrently by several services; retry races on catalog rows.
	var lastErr error
	for i := 0; i < 10; i++ {
		if _, lastErr = pool.Exec(ctx, schemaSQL); lastErr == nil {
			return pool, nil
		}
		time.Sleep(500 * time.Millisecond)
	}
	return nil, fmt.Errorf("apply schema: %w", lastErr)
}
