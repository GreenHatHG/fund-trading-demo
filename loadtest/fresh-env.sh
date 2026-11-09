#!/usr/bin/env bash
# Wipe everything (services + postgres volume) and start a fresh environment.
# Usage: bash loadtest/fresh-env.sh
set -euo pipefail
cd "$(dirname "$0")/.."

echo "== 1. stop old services =="
pkill -f 'go run' 2>/dev/null || true
pkill -f 'mix run' 2>/dev/null || true
sleep 1
pkill -9 -f 'go run' 2>/dev/null || true
pkill -9 -f 'mix run' 2>/dev/null || true

echo "== 2. stop postgres and DELETE its volume =="
docker compose down -v --remove-orphans

echo "== 3. start fresh postgres =="
docker compose up -d
for i in $(seq 1 30); do
  if docker compose ps postgres | grep -q healthy; then break; fi
  sleep 1
done
docker compose ps postgres | grep -q healthy || { echo "postgres failed to get healthy"; exit 1; }

echo "== 4. compile check =="
go build ./...

echo "== 5. start all services =="
mkdir -p /tmp/fund-fresh-logs
nohup go run ./downstream-mock > /tmp/fund-fresh-logs/downstream-mock.log 2>&1 &
nohup go run ./api            > /tmp/fund-fresh-logs/api.log            2>&1 &
nohup go run ./reconciliation > /tmp/fund-fresh-logs/reconciliation.log 2>&1 &
(cd job_worker_elixir && nohup mix run --no-halt > /tmp/fund-fresh-logs/job-worker.log 2>&1 &)

echo "== 6. wait for api :8080 =="
for i in $(seq 1 60); do
  if curl -s -o /dev/null localhost:8080/debug/trace/x; then break; fi
  sleep 1
done
curl -s -o /dev/null localhost:8080/debug/trace/x && echo "api is up" || { echo "api did not come up"; tail -n 30 /tmp/fund-fresh-logs/*.log; exit 1; }

echo
echo "Fresh environment ready. Logs: /tmp/fund-fresh-logs/"
echo "Next: k6 run loadtest/intake.js"
