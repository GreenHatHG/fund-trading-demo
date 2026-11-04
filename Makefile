.PHONY: run build test demo

build:
	go build ./...

test:
	go test ./...

# starts everything for a local demo (assumes docker compose postgres is up)
run:
	go run ./downstream-mock & go run ./api & go run ./job-worker & go run ./reconciliation &
