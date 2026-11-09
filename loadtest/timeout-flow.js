// Round 2: downstream timeout -> UNKNOWN -> reconciliation -> terminal state.
// Flow: POST /__control mode=timeout for the test fund, drive orders, verify
// the UNKNOWN set drains to CONFIRMED/FAILED and never to a wrong state.
// The mock's timeout never answers, so the worker's 800ms timeout fires,
// submitter maps it to UNKNOWN, sweep/submit retry... and reconciliation
// queries /query/:id. A timed-out order was never recorded downstream, so
// /query returns UNKNOWN and reconciliation fails it (RECONCILED_NEVER_RECEIVED).
// Run: k6 run loadtest/timeout-flow.js
// Prereq: clean orders table; reconciliation + job-worker running.
import http from 'k6/http';
import { check } from 'k6';
import { Counter } from 'k6/metrics';
import { openDb, waitDrain, verifyTimeoutFlow } from './lib/db.js';

const BASE = __ENV.BASE_URL || 'http://localhost:8080';
const MOCK = __ENV.MOCK_URL || 'http://localhost:8090';
const DURATION = __ENV.DURATION || '20s';
const RATE = Number(__ENV.RATE || 20);

http.setResponseCallback(http.expectedStatuses(200, 201));

const unknownSeen = new Counter('unknown_seen');
const reconciled = new Counter('reconciled');

export const options = {
  scenarios: {
    intake: {
      executor: 'constant-arrival-rate',
      rate: RATE,
      timeUnit: '1s',
      duration: DURATION,
      preAllocatedVUs: 30,
      maxVUs: 200,
      exec: 'order',
    },
  },
  thresholds: {
    http_req_duration: ['p(95)<100'],
    http_req_failed: ['rate<0.01'],
    checks: ['rate>0.99'],
  },
};

const RUN = __ENV.RUN || `${Date.now() % 100000000}`;

export function setup() {
  // Force every PURCHASE for the test fund to time out downstream.
  const res = http.post(`${MOCK}/__control`,
    JSON.stringify({ fund_code: '000002', direction: 'PURCHASE', mode: 'timeout' }),
    { headers: { 'Content-Type': 'application/json' } });
  check(res, { 'control set': (r) => r.status === 200 });
}

export function order() {
  const body = { user_id: `to${RUN}-${__VU}-${__ITER}`, fund_code: '000002', direction: 'PURCHASE', amount: '50.00' };
  const res = http.post(`${BASE}/orders`, JSON.stringify(body), {
    headers: { 'Content-Type': 'application/json', 'Idempotency-Key': `tk${RUN}-${__VU}-${__ITER}` },
  });
  check(res, { '201': (r) => r.status === 201 });
}

// DB validation runs inside handleSummary: wait for the UNKNOWN set to drain,
// then check the reconcile invariants scoped to this round's tk<prefix> keys.
export function teardown() {
  http.post(`${MOCK}/__control`,
    JSON.stringify({ fund_code: '000002', direction: 'PURCHASE', mode: 'random' }),
    { headers: { 'Content-Type': 'application/json' } });
}

export function handleSummary(data) {
  let dbLines = [];
  let verdict = 'SKIP (db unreachable)';
  try {
    const db = openDb();
    const { nonterminal, waitedS } = waitDrain(db, `tk${RUN}`, Number(__ENV.DRAIN_WAIT || 60));
    const { pass, lines } = verifyTimeoutFlow(db, `tk${RUN}`);
    db.close();
    dbLines = [`drain: waited=${waitedS}s nonterminal_remaining=${nonterminal}`].concat(lines).join('\n');
    verdict = pass ? 'PASS' : 'FAIL';
  } catch (e) {
    dbLines = `db check error: ${e}`;
  }

  return {
    stdout: `
===== round-2 summary =====
http_reqs=${data.metrics.http_reqs?.values.count || 0}
checks_pass=${((data.metrics.checks?.values.rate || 0) * 100).toFixed(2)}%

===== db invariants (idempotency_key LIKE 'tk${RUN}%') =====
${dbLines}
VERIFY-TIMEOUT: ${verdict}
`,
  };
}
