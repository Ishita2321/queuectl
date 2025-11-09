#!/usr/bin/env bash
set -euo pipefail
# Example quick validation script

# ensure a clean DB for the test (be careful)
rm -f queue.db
rm -f queuectl_workers.pid

echo "Enqueue a successful job (echo)..."
./queuectl.py enqueue '{"id":"job-success","command":"echo hello && sleep 1"}'

echo "Enqueue a failing job (unknown command) that will retry..."
./queuectl.py enqueue '{"id":"job-fail","command":"false","max_retries":2}'

echo "Enqueue a long job..."
./queuectl.py enqueue '{"id":"job-sleep","command":"sleep 3","max_retries":1}'

echo "Start workers in background (2 workers). You can stop with ./queuectl.py worker stop"
# start in background: we use nohup to detach for test convenience
nohup ./queuectl.py worker start --count 2 > queuectl.log 2>&1 &
sleep 1

echo "Status at start:"
./queuectl.py status
echo "Waiting 10 seconds for processing..."
sleep 10

echo "Status after wait:"
./queuectl.py status

echo "List completed:"
./queuectl.py list --state completed

echo "List dead (DLQ):"
./queuectl.py dlq list

echo "Stop workers"
./queuectl.py worker stop
sleep 2
echo "Final status:"
./queuectl.py status

echo "Done. View logs in queuectl.log"
