// Shared DB-invariant checks run from inside k6 (k6/x/sql, postgres driver).
// Each load-test script calls verifyRound() in handleSummary with its own
// idempotency-key prefix, so every run validates exactly the data it wrote.
import sql from 'k6/x/sql';
import driver from 'k6/x/sql/driver/postgres';
import { sleep } from 'k6';

const DSN = __ENV.DATABASE_URL || 'postgres://fund:fund@localhost:5432/fund?sslmode=disable';

export function openDb() {
  return sql.open(driver, DSN);
}

function q(db, query, ...args) {
  return db.query(query, ...args).map((r) => Object.values(r));
}

// Run all round-1 (intake) invariants scoped to idempotency keys like `${prefix}%`.
// Returns { pass, lines } — the caller prints and decides the exit code.
export function verifyIntake(db, prefix) {
  const like = `${prefix}%`;
  const lines = [];
  let pass = true;
  const add = (name, count, limit) => {
    const ok = Number(count) <= limit;
    pass = pass && ok;
    lines.push(`${ok ? 'ok' : 'FAIL'}  ${name}=${count}`);
  };

  const [dup] = q(db,
    'SELECT count(*) FROM (SELECT 1 FROM orders WHERE idempotency_key LIKE $1 GROUP BY user_id, idempotency_key HAVING count(*)>1) d', like)[0];
  add('duplicate (user_id, idempotency_key)', dup, 0);

  const [bad] = q(db,
    "SELECT count(*) FROM orders WHERE idempotency_key LIKE $1 AND (amount<=0 OR fund_code='' OR direction NOT IN ('PURCHASE','REDEEM'))", like)[0];
  add('invalid rows', bad, 0);

  // round-trip: intake only ever writes amount 100.00 / fund 000001 under its keys
  const [rt] = q(db,
    "SELECT count(*) FROM orders WHERE idempotency_key LIKE $1 AND (amount <> '100.00' OR fund_code <> '000001')", like)[0];
  add('round-trip mismatch', rt, 0);

  // CREATED orders older than 120s must have a SUBMIT job (younger = pre-enqueue backlog)
  const [orphan] = q(db, `
    SELECT count(*) FROM orders o
    WHERE o.state='CREATED' AND o.created_at < now() - interval '120 seconds'
      AND o.idempotency_key LIKE $1 AND NOT EXISTS (
        SELECT 1 FROM jobs j WHERE j.order_id=o.id AND j.job_type='SUBMIT')`, like)[0];
  add('CREATED without SUBMIT job (>120s)', orphan, 0);

  const [ev] = q(db, `
    SELECT count(*) FROM (
      SELECT o.id FROM orders o LEFT JOIN order_events e
        ON e.order_id=o.id AND e.event_type='ORDER_CREATED'
      WHERE o.idempotency_key LIKE $1
      GROUP BY o.id HAVING count(e.id)<>1) x`, like)[0];
  add('orders without exactly one ORDER_CREATED', ev, 0);

  const [unknown] = q(db,
    "SELECT count(*) FROM orders WHERE state='UNKNOWN' AND idempotency_key LIKE $1 AND updated_at < now() - interval '15 seconds'", like)[0];
  add('stale UNKNOWN (>15s)', unknown, 0);

  // Per-row consistency: pull the violating order ids, not just counts.
  pass = pass && addBadRows(db, lines, like);

  return { pass, lines };
}

// Row-level checks. Each query returns the order ids that break one rule.
// Empty list = every row of this round is individually correct.
// Returns false if any rule found bad rows.
function addBadRows(db, lines, like) {
  let ok = true;
  const rule = (rows, name) => {
    if (rows.length === 0) {
      lines.push(`ok  every row: ${name}`);
      return;
    }
    ok = false;
    lines.push(`FAIL ${name}: ${rows.length} row(s)`);
    for (const [r, id, detail] of rows.slice(0, 10)) {
      lines.push(`      order ${id}: ${detail} (${r})`);
    }
  };

  // state must have a matching event: ORDER_CREATED always; non-CREATED states
  // must carry the event that produced them.
  rule(q(db, `
    SELECT 'order_state_vs_events' AS rule, o.id::text, o.state
    FROM orders o
    WHERE o.idempotency_key LIKE $1
      AND o.state <> 'CREATED' AND NOT EXISTS (
        SELECT 1 FROM order_events e WHERE e.order_id=o.id
          AND e.event_type IN (
            'SUBMITTED','REJECTED','DOWNSTREAM_TIMEOUT','UNEXPECTED_RESPONSE',
            'TIMEOUT_SWEEP','RECONCILED_ACCEPTED','RECONCILED_REJECTED','RECONCILED_NEVER_RECEIVED'))`, like),
    'non-CREATED order has a state-change event');

  // version counts state changes: it starts at 0 on insert (the ORDER_CREATED
  // event is written without a bump), and every Transition bumps it by 1 and
  // appends exactly one state-change event. So version == non-CREATED event count.
  rule(q(db, `
    SELECT 'version_vs_events' AS rule, o.id::text, o.version || ' vs ' || count(e.id)
    FROM orders o LEFT JOIN order_events e ON e.order_id=o.id AND e.event_type <> 'ORDER_CREATED'
    WHERE o.idempotency_key LIKE $1
    GROUP BY o.id, o.version HAVING o.version <> count(e.id)`, like),
    'version equals state-change event count');

  // jobs: non-CREATED order -> SUBMIT job DONE (or DEAD after retries);
  // CREATED order -> SUBMIT job still PENDING/RUNNING (or not enqueued yet).
  // Small grace: the worker marks DONE right after the submit task returns, so
  // a row can be non-CREATED while its job is mid-claim — retry briefly before
  // declaring a violation.
  let jrows = q(db, `
    SELECT 'job_state_vs_order_state' AS rule, o.id::text, o.state || '/' || j.state
    FROM orders o JOIN jobs j ON j.order_id=o.id AND j.job_type='SUBMIT'
    WHERE o.idempotency_key LIKE $1
      AND ( (o.state <> 'CREATED' AND j.state NOT IN ('DONE','DEAD'))
         OR (o.state = 'CREATED' AND j.state NOT IN ('PENDING','RUNNING')) )`, like);
  for (let i = 0; i < 5 && jrows.length > 0; i++) {
    sleep(1);
    jrows = q(db, `
      SELECT 'job_state_vs_order_state' AS rule, o.id::text, o.state || '/' || j.state
      FROM orders o JOIN jobs j ON j.order_id=o.id AND j.job_type='SUBMIT'
      WHERE o.idempotency_key LIKE $1
        AND ( (o.state <> 'CREATED' AND j.state NOT IN ('DONE','DEAD'))
           OR (o.state = 'CREATED' AND j.state NOT IN ('PENDING','RUNNING')) )`, like);
  }
  rule(jrows, 'job state vs order state');

  return ok;
}

// Poll until every order under `prefix` is terminal (CONFIRMED/FAILED) —
// the round-2 drain check (UNKNOWN must resolve via reconciliation).
export function waitDrain(db, prefix, maxWaitS = 60) {
  const like = `${prefix}%`;
  const start = Date.now();
  let nonterminal = -1;
  while ((Date.now() - start) / 1000 < maxWaitS) {
    [nonterminal] = q(db,
      "SELECT count(*) FROM orders WHERE idempotency_key LIKE $1 AND state IN ('CREATED','SUBMITTED','UNKNOWN')", like)[0];
    if (Number(nonterminal) === 0) break;
    sleep(3);
  }
  return { nonterminal, waitedS: Math.round((Date.now() - start) / 1000) };
}

// Round-2 invariants after drain: terminal + reconcile evidence, scoped to prefix.
export function verifyTimeoutFlow(db, prefix) {
  const like = `${prefix}%`;
  const lines = [];
  let pass = true;
  const add = (name, count, ok) => {
    pass = pass && ok;
    lines.push(`${ok ? 'ok' : 'FAIL'}  ${name}=${count}`);
  };

  const [nonterminal] = q(db,
    "SELECT count(*) FROM orders WHERE idempotency_key LIKE $1 AND state IN ('CREATED','SUBMITTED','UNKNOWN')", like)[0];
  add('nonterminal after drain', nonterminal, Number(nonterminal) === 0);

  const [recon] = q(db, `
    SELECT count(*) FROM order_events e JOIN orders o ON o.id=e.order_id
    WHERE o.idempotency_key LIKE $1 AND e.event_type='RECONCILED_NEVER_RECEIVED'`, like)[0];
  add('RECONCILED_NEVER_RECEIVED events', recon, Number(recon) > 0); // vacuous pass is a failure

  const [multi] = q(db, `
    SELECT count(*) FROM (
      SELECT o.id FROM order_events e JOIN orders o ON o.id=e.order_id
      WHERE o.idempotency_key LIKE $1 AND e.event_type LIKE 'RECONCILED_%'
      GROUP BY o.id HAVING count(*)>1) x`, like)[0];
  add('orders with >1 RECONCILED_* event', multi, Number(multi) === 0);

  return { pass, lines };
}
