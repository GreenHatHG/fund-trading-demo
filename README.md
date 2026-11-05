# job-worker (Elixir port)

Same job queue as the Go `job-worker`: claims jobs with `FOR UPDATE SKIP LOCKED`,
submits orders to downstream-mock over HTTP, retries with exponential backoff,
sweeps stale SUBMITTED orders to UNKNOWN.

OTP pieces:
- `JobWorker.Repo` — GenServer wrapping a Postgrex connection
- `JobWorker.Poller` — GenServer tick loop; each job runs in a supervised Task
  (`Task.Supervisor.async_nolink`), so a crashed submit can't kill the poller
- `JobWorker.OrderCore` — the state machine port; ALL order state writes go
  through `transition/6` (optimistic lock + event append in one transaction)

Run (postgres + mock + api must be up):
    cd job_worker_elixir && mix run --no-halt

Hard constraints kept: timeout -> UNKNOWN, never FAILED; sweep writes only via
the state machine; idempotent handlers (re-delivered job on non-CREATED order is a no-op).
