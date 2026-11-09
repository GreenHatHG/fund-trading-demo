# Fault-Tolerance Test Design (modeled on MIT 6.824 test suites)

Reference: `~/projects/MIT6.824/src/raft/test_test.go`, `kvraft/test_test.go`.

## What 6.824 tests teach us

1. **A test harness owns the faults.** 6.824 wraps every peer in `cfg` with
   `connect()`, `disconnect()`, `crash()`, `start1()`, and an `unreliable`
   flag on the RPC layer. Faults are injected by the harness, never by the
   code under test. Our equivalent: a test-only fault layer between
   job-worker / reconciliation and the downstream mock (`FAULT_*` env vars on
   `downstream-mock`, or a proxy). No production code changes.
2. **Every test ends with an invariant check, not a log check.**
   `checkOneLeader()`, `checkTerms()`, `checkConcurrentAppends()` verify
   *end state*. Ours: after every fault scenario, assert every order is
   terminal and correct (`CONFIRMED`/`FAILED`), never stuck, never wrong.
3. **Timeout means UNKNOWN, and tests prove it.** Raft tests crash the leader
   mid-agreement and check no entry is lost. Ours: force `mode:"timeout"`,
   assert the order goes to `UNKNOWN` (not `FAILED`), then let reconciliation
   resolve it — and prove reconciliation only queries, never resubmits.
4. **Crash + restart must be safe.** `TestPersist*` / `TestSnapshotRecover*`
   kill servers and restart them. Ours: kill api / reconciliation mid-flight,
   restart, assert no duplicate submission, no lost order.
5. **Unreliable + concurrent clients.** `TestUnreliable3A`,
   `TestPersistConcurrentUnreliable3A` run many clients against a lossy
   network. Ours: replay the same `Idempotency-Key` N times concurrently and
   N distinct keys concurrently while the mock randomly times out.
6. **Non-determinism is bounded by retries + generous deadlines.** 6.824
   polls in a loop with a timeout instead of a single sleep. Follow the same
   pattern: `waitFor(t, 10s, func() bool { ... })`.

## Harness sketch

```go
// internal/faulttest/harness.go (test-only)
type Harness struct {
    t       *testing.T
    dbURL   string
    mockURL string
}

// Fault controls (map to downstream-mock /__control):
//   h.forceMode(fundCode, dir, "timeout" | "fail" | "success")
//   h.clearMode(fundCode, dir)
// Process control (make run / docker compose):
//   h.stopService("reconciliation"); h.startService("reconciliation")
func (h *Harness) waitFor(deadline time.Duration, cond func() bool)
func (h *Harness) assertAllTerminal()
func (h *Harness) assertState(orderID int64, want string)
```

Prerequisite: split tests that need process control into an
integration suite (tagged `-tags=integration`, run by `make test-integration`
after `docker compose up -d && make run`), so `go test ./...` stays fast and
deterministic.

## Test cases

### A. State-machine safety (unit, no infra) — mirrors `TestMachineAllowedPaths`

| # | Name | Fault injected | Pass criteria |
|---|------|----------------|---------------|
| A1 | `TestTimeoutNeverMapsToFailed` | submit client returns `context.DeadlineExceeded` | state = `UNKNOWN`, error is not a submit-failure |
| A2 | `TestUnknownOnlyResolvesViaReconcile` | direct `Transition(Unknown→Submitted)` | rejected (`ErrInvalidTransition`) — never resubmit |
| A3 | `TestDoubleReconcileIdempotent` | two reconcile passes, same order | second pass is a no-op; still `CONFIRMED`, one audit row |

### B. Downstream faults (integration, via `/__control`) — mirrors `TestFailAgree`, `TestRejoin`

| # | Name | Fault injected | Pass criteria |
|---|------|----------------|---------------|
| B1 | `TestSubmitTimeoutThenDownstreamAccepted` | `/__control mode:"timeout"`, then clear; order actually ACCEPTED downstream | UNKNOWN → reconcile → `CONFIRMED` within `waitFor` deadline |
| B2 | `TestSubmitTimeoutThenDownstreamRejected` | same, but mode `"fail"` at submit, `"success"` on query path | UNKNOWN → reconcile → `FAILED` with reason `RECONCILED_REJECTED` |
| B3 | `TestSubmitTimeoutDownstreamNeverSaw` | timeout + the mock never recorded the order | reconcile → `FAILED` reason `RECONCILED_NEVER_RECEIVED`; exactly 1 downstream submission (count via mock's query log) |
| B4 | `TestReconcileQueryDown` | force timeout mode, stop mock before reconcile tick | order stays `UNKNOWN` across ticks (retry loop, no wrong terminal state); start mock → resolves |
| B5 | `TestReconcileQueryNon200` | mock returns 500 for `/query/:id` | order stays `UNKNOWN`; no transition attempted |

### C. Crash / restart — mirrors `TestPersist1`, `TestSnapshotRecover`

| # | Name | Fault injected | Pass criteria |
|---|------|----------------|---------------|
| C1 | `TestKillWorkerMidSubmit` | `pkill -f submitter` (or kill job-worker process) right after it marks `SUBMITTED`, order ACCEPTED downstream | restart worker → no resubmission of the same order (mock submission count == 1) → reconcile to `CONFIRMED` |
| C2 | `TestKillApiAfterInsertBeforeResponse` | kill api between DB write and HTTP response | client retries with same `Idempotency-Key` → 200 replay, one order row (`(user_id, idempotency_key)` unique index) |
| C3 | `TestKillReconciliationMidBatch` | kill reconciliation while it processes 100 UNKNOWNs | restart → remaining UNKNOWNs resolved; none double-transitioned (audit log has ≤1 transition per order per path) |
| C4 | `TestRestartAfterFullCrash` | stop everything (`pkill -f 'go run'`), restart | all pre-crash orders still resolvable to terminal states; schema idempotent on re-apply |

### D. Concurrency + unreliable network — mirrors `TestUnreliable3A`, `TestConcurrent3A`

| # | Name | Fault injected | Pass criteria |
|---|------|----------------|---------------|
| D1 | `TestConcurrentReplaySameKey` | 20 goroutines POST same `Idempotency-Key` simultaneously (no mock fault) | all get 200 + identical order body + `"replay":true` on losers; exactly 1 row |
| D2 | `TestConcurrentDistinctKeys` | 50 distinct orders, mock random 10% timeout (its default mix) | `waitFor`: all 50 terminal; counts: confirmed+failed == 50; zero UNKNOWN left |
| D3 | `TestReconcileRaceWithQueryTimeout` | reconcile query client timeout set shorter than mock's slow response | query failure → stays UNKNOWN → retried next tick → resolves; never `FAILED` from a mere timeout |
| D4 | `TestBatchSubmitOverlap` | two job-worker ticks overlap (shrink tick interval to 100ms) | no order submitted twice (mock submission count == 1 per order) |

### F. Database faults — mirrors TestPersist*, but against real storage

6.824 models crash-recovery in memory (labgob); we have real Postgres, so we
can go further. The system's correctness claim is "every order eventually
reaches a terminal state, no double submission" — DB crashes must not break
it.

#### Fault injection levels (all harness-side, no production code changes)

1. **Process kill** — `docker kill --signal=SIGKILL <pg>` (SIGKILL, not
   `stop`: stop does a clean shutdown, which is the *easy* case).
   Recovery: `docker compose up -d postgres` and wait for `healthy`.
2. **Connection drop** — kill the connection between a service and Postgres:
   `SELECT pg_terminate_backend(pid)` for that service's backends (query
   `pg_stat_activity` by `application_name`/`usename`), or cancel the
   service's `context.Context` mid-transaction. Simulates network partition
   to the DB. Services must reconnect via the pool (pgxpool does this
   automatically) and retry.
3. **Transaction abort** — inject a failure between the `UPDATE orders` and
   the `INSERT order_events` inside `Transition` (test-only hook or a
   wrapper `Pool` in the harness that errors on the Nth query). The whole
   tx must roll back: state unchanged, no orphan event row.
4. **Commit-unknown** — the DB analog of a submit timeout: kill the
   connection *after* the app sends `COMMIT` but before it reads the ack
   (`ctx cancel` racing the commit). The tx may or may not have committed —
   exactly the UNKNOWN semantics. Test: the caller treats the error as
   retryable-read-then-decide, not as a definite failure; final state is
   consistent with what a fresh read shows.
5. **Transient errors** — make a wrapped `Pool` return a few
   `pgconn.SafeToRetry`-style errors / serialization failures / pool
   exhaustion (`pool.SetMaxConns(1)` to force contention) before letting
   queries through. Callers must retry and converge.

#### Test cases

| # | Name | Fault injected | Pass criteria |
|---|------|----------------|---------------|
| F1 | `TestPGKillDuringTraffic` | SIGKILL postgres while api + worker are taking load (reuse D2 load) | services log errors but recover; after PG restarts, `waitFor` → all orders terminal; `assertInvariants` passes |
| F2 | `TestKillBetweenUpdateAndEventInsert` | abort the tx after the `UPDATE` inside `Transition` (hook) | order state unchanged, `version` unchanged, zero `order_events` rows for that attempt; a retry succeeds cleanly |
| F3 | `TestCommitAckLost` | cancel ctx right at `COMMIT` (racy: run 20 iterations, expect some to land) | for each iteration: fresh read matches outcome — no order stuck in a phantom state; no event row without the matching state (atomicity) |
| F4 | `TestDropConnsMidBatch` | `pg_terminate_backend` on all worker connections mid submit-batch | worker's next tick picks up still-pending orders; no order submitted twice (mock submission count) |
| F5 | `TestUniqueIndexUnderDBFaults` | concurrent same-key replay (D1) *plus* F2-style tx aborts racing | unique index `(user_id, idempotency_key)` holds: exactly 1 row ever; losers get the original order back (replay 200) even when their own insert was aborted |
| F6 | `TestRestartSchemaIdempotent` | restart a service twice against an existing DB | schema re-apply is a no-op (no errors, no data loss); pre-existing orders intact |

F3 and F5 are the two hardest and the most valuable: F3 tests atomicity of
the state+event pair under commit-unknown, F5 tests that durability of the
idempotency guarantee doesn't depend on any single transaction succeeding.

### E. End-to-end invariants (run after every scenario above)

Shared checker, like 6.824's `check()` helpers:

```go
func (h *Harness) assertInvariants() {
    // 1. every order in {CONFIRMED, FAILED} — no CREATED/SUBMITTED/UNKNOWN left
    // 2. no FAILED order was ever ACCEPTED downstream (query mock for its id)
    // 3. every CONFIRMED order has ≥1 successful downstream submission record
    // 4. audit/state_transitions: no order leaves a terminal state
    // 5. no order has more than one submission per (order_id)
}
```

## Mapping table (6.824 concept → this project)

| 6.824 | Here |
|---|---|
| `cfg.disconnect(i)` / partition | `/__control mode:"timeout"` (partial partition to downstream) |
| `cfg.crash(i)` + `cfg.start1(i)` | `pkill` / restart the service process |
| `unreliable()` RPC layer | mock's random outcome mix (70/20/10) |
| `checkOneLeader()` | `assertAllTerminal()` |
| `NextValue(prev, val)` model | expected order states derived from mock's query log |
| porcupine linearizability | (out of scope for demo) D1/D2 invariants are the light version |
| `wait(timeout)` polling loop | `Harness.waitFor` |

## Suggested file layout

```
internal/ordercore/fault_paths_test.go     # A1–A3 (pure unit)
downstream-mock/  (add /__stats for submission counts — test-only endpoint)
faulttest/  (or test/integration/)
  harness_test.go
  downstream_faults_test.go                # B1–B5  (build tag: integration)
  crash_restart_test.go                    # C1–C4  (build tag: integration)
  concurrency_test.go                      # D1–D4  (build tag: integration)
Makefile: test-integration target
```

## Priority

P0 first (data-loss / state-corruption paths, per AGENTS.md):
**A1, A2, B1, B3, C1, C2, F2, F5, E**. Then F1/F3, B4/B5, D1/D2, the rest.
