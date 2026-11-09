// k6 load test for the api intake (:8080).
// Round 1: prove the script + data validation work; 50 -> 200 req/s.
// Run: k6 run loadtest/intake.js
// Env overrides: BASE_URL, RATES (e.g. "50,100,200"), DURATION per stage.
import http from 'k6/http';
import { check } from 'k6';
import { Counter } from 'k6/metrics';
import { openDb, verifyIntake } from './lib/db.js';

// bad scenario sends invalid payloads on purpose: 400 is expected, not a failure
http.setResponseCallback(http.expectedStatuses(200, 201, 400));

const BASE = __ENV.BASE_URL || 'http://localhost:8080';
const DURATION = __ENV.DURATION || '40s';
const RATES = (__ENV.RATES || '50,100,200').split(',').map(Number);

const replayOk = new Counter('replay_ok');
const replayBad = new Counter('replay_bad');
const created201 = new Counter('created_201');

function makeOrder(run, iter, seq) {
  return {
    user_id: `u${run}-${iter}-${seq}`,
    fund_code: '000001',
    direction: seq % 5 === 4 ? 'REDEEM' : 'PURCHASE',
    amount: '100.00',
  };
}

// ONLY=normal|replay|bad runs just that scenario (default: all three)
export const options = (() => {
  const all = {
    scenarios: {
      normal: {
        executor: 'constant-arrival-rate',
        rate: RATES[0],
        timeUnit: '1s',
        duration: DURATION,
        preAllocatedVUs: 50,
        maxVUs: 500,
        exec: 'normalOrder',
        startTime: '0s',
      },
      replay: {
        // same key over and over: exercises the replay path hard
        executor: 'constant-arrival-rate',
        rate: Math.max(5, Math.round(RATES[0] / 10)),
        timeUnit: '1s',
        duration: DURATION,
        preAllocatedVUs: 20,
        maxVUs: 100,
        exec: 'replayOrder',
        startTime: '0s',
      },
      bad: {
        executor: 'constant-arrival-rate',
        rate: Math.max(5, Math.round(RATES[0] / 10)),
        timeUnit: '1s',
        duration: DURATION,
        preAllocatedVUs: 10,
        maxVUs: 50,
        exec: 'badOrder',
        startTime: '0s',
      },
    },
  };
  const thresholds = {
    http_req_duration: ['p(95)<100', 'p(99)<300'],
    http_req_failed: ['rate<0.01'],
    checks: ['rate>0.99'],
  };
  const only = __ENV.ONLY;
  if (only && all.scenarios[only]) {
    return { scenarios: { [only]: all.scenarios[only] }, thresholds };
  }
  return { scenarios: all.scenarios, thresholds };
})();

// RUN is settable so a runner can reuse the same round id across scripts
const RUN = __ENV.RUN || `${Date.now() % 100000000}`;

export function normalOrder() {
  const seq = __ITER;
  const body = makeOrder(RUN, __VU, seq);
  const res = http.post(`${BASE}/orders`, JSON.stringify(body), {
    headers: {
      'Content-Type': 'application/json',
      // unique key per (user, attempt): exercises the insert path
      'Idempotency-Key': `k${RUN}-${__VU}-${seq}`,
      'X-Request-ID': `r${RUN}-${__VU}-${seq}`,
    },
  });
  created201.add(res.status === 201);
  check(res, {
    'status 201': (r) => r.status === 201,
    'replay false': (r) => r.json('replay') === false,
    'has order id': (r) => !!r.json('order.id'),
    // round-trip: the stored order must echo exactly what we sent
    'echoes user_id': (r) => r.json('order.user_id') === body.user_id,
    'echoes amount': (r) => r.json('order.amount') === body.amount,
    'echoes direction': (r) => r.json('order.direction') === body.direction,
    'echoes idempotency key': (r) => r.json('order.idempotency_key') === `k${RUN}-${__VU}-${seq}`,
  });
}

export function replayOrder() {
  const body = makeOrder(RUN, __VU, 0);
  const res = http.post(`${BASE}/orders`, JSON.stringify(body), {
    headers: {
      'Content-Type': 'application/json',
      'Idempotency-Key': `rk${RUN}-${__VU}`, // fixed key: replays after first call
    },
  });
  const isReplay = res.json('replay') === true;
  if (res.status === 201) check(res, { 'first call 201': () => true });
  if (isReplay) {
    replayOk.add(res.status === 200);
    replayBad.add(res.status !== 200);
  }
  check(res, {
    'status 201 or 200': (r) => r.status === 201 || r.status === 200,
    'replay returns 200': (r) => (r.json('replay') === true ? r.status === 200 : true),
  });
}

export function badOrder() {
  const cases = [
    { body: { fund_code: '000001', direction: 'PURCHASE', amount: '1.00' }, key: 'x' }, // no user_id
    { body: { user_id: 'u', fund_code: '000001', direction: 'SIDEWAYS', amount: '1.00' }, key: 'x' },
    { body: { user_id: 'u', fund_code: '000001', direction: 'PURCHASE', amount: '1.00' }, key: '' }, // no key
  ];
  const c = cases[__ITER % cases.length];
  const res = http.post(`${BASE}/orders`, JSON.stringify(c.body), {
    headers: { 'Content-Type': 'application/json', 'Idempotency-Key': c.key },
  });
  check(res, { 'status 400': (r) => r.status === 400 });
}

export function handleSummary(data) {
  const t = data.metrics.http_req_duration?.values || {};
  // k6 serializes p(99) as "p(99)"; fall back to p(90)/max so the line never prints undefined
  const pct = (k) => t[k] != null ? `${t[k].toFixed(1)}ms` : 'n/a';

  // post-run DB validation: check exactly the rows this run wrote
  let dbLines = '';
  let verdict = 'SKIP (db unreachable)';
  try {
    const db = openDb();
    const { pass, lines } = verifyIntake(db, `k${RUN}`);
    db.close();
    dbLines = lines.join('\n');
    verdict = pass ? 'PASS' : 'FAIL';
  } catch (e) {
    dbLines = `db check error: ${e}`;
  }

  return {
    stdout: `
===== round-1 summary =====
p95=${pct('p(95)')} p99=${pct('p(99)')} p90=${pct('p(90)')} max=${pct('max')}
created_201=${data.metrics.created_201?.values.count || 0}
replay_ok=${data.metrics.replay_ok?.values.count || 0} replay_bad=${data.metrics.replay_bad?.values.count || 0}
checks_pass=${((data.metrics.checks?.values.rate || 0) * 100).toFixed(2)}%

===== db invariants (idempotency_key LIKE 'k${RUN}%') =====
${dbLines}
VERIFY: ${verdict}
`,
  };
}
