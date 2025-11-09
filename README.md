# queuectl — CLI background job queue (Intern assignment)

## Summary
`queuectl` is a minimal production-like job queue implemented in Python. It supports:
- Enqueue jobs specifying an `id` and `command`
- Start workers (multiple threads) to process jobs
- Retry failed jobs using exponential backoff and move to DLQ after exhausting retries
- Persistent storage using SQLite
- Graceful shutdown of workers
- CLI commands to view status, list jobs, manage DLQ, and set config

## Requirements
- Python 3.9+
- Unix-like shell (Linux, macOS). Works on Windows with minor adjustments.

## Files
- `queuectl.py` — main CLI
- `test_flow.sh` — quick test script
- `queue.db` — created automatically
- `queuectl_workers.pid` — PID file for running worker master

## Quick start
```bash
chmod +x queuectl.py
./queuectl.py enqueue '{"id":"job1","command":"sleep 2"}'
./queuectl.py worker start --count 2   # runs workers in foreground
# or run in background
nohup ./queuectl.py worker start --count 2 > queuectl.log 2>&1 &

./queuectl.py status
./queuectl.py list --state pending
./queuectl.py dlq list
./queuectl.py dlq retry job1
./queuectl.py worker stop
