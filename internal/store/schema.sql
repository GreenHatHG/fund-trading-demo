-- schema.sql — run at startup by any Go service (idempotent).
CREATE TABLE IF NOT EXISTS orders (
    id             BIGSERIAL PRIMARY KEY,
    user_id        TEXT NOT NULL,
    idempotency_key TEXT NOT NULL,
    fund_code      TEXT NOT NULL,
    direction      TEXT NOT NULL CHECK (direction IN ('PURCHASE', 'REDEEM')),
    amount         NUMERIC(14,2) NOT NULL,
    state          TEXT NOT NULL CHECK (state IN ('CREATED','SUBMITTED','CONFIRMED','FAILED','UNKNOWN')),
    trading_day    DATE NOT NULL,
    confirm_date   DATE,
    version        INT NOT NULL DEFAULT 0,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE UNIQUE INDEX IF NOT EXISTS orders_idem_idx ON orders (user_id, idempotency_key);
CREATE INDEX IF NOT EXISTS orders_state_idx ON orders (state) WHERE state IN ('CREATED','UNKNOWN');

CREATE TABLE IF NOT EXISTS order_events (
    id         BIGSERIAL PRIMARY KEY,
    order_id   BIGINT NOT NULL REFERENCES orders(id),
    event_type TEXT NOT NULL,
    payload    JSONB NOT NULL DEFAULT '{}',
    request_id TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS order_events_order_idx ON order_events (order_id);

CREATE TABLE IF NOT EXISTS jobs (
    id          BIGSERIAL PRIMARY KEY,
    order_id    BIGINT NOT NULL REFERENCES orders(id),
    job_type    TEXT NOT NULL CHECK (job_type IN ('SUBMIT','RECONCILE')),
    state       TEXT NOT NULL CHECK (state IN ('PENDING','RUNNING','DONE','DEAD')),
    attempts    INT NOT NULL DEFAULT 0,
    max_attempts INT NOT NULL DEFAULT 5,
    run_after   TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS jobs_due_idx ON jobs (state, run_after);
-- one order's jobs processed serially
CREATE UNIQUE INDEX IF NOT EXISTS jobs_active_idx ON jobs (order_id) WHERE state IN ('PENDING','RUNNING');
