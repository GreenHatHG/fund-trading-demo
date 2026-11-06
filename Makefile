.PHONY: run build test demo

build:
	go build ./...

test:
	go test ./...

# starts everything for a local demo (assumes docker compose postgres is up)
# job-worker is Elixir now (job_worker_elixir); the rest are Go services.
run:
	go run ./downstream-mock & go run ./api & cd job_worker_elixir && mix run --no-halt & go run ./reconciliation &
