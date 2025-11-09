#!/usr/bin/env python3
"""
queuectl.py — Minimal job queue CLI for the assignment.

Usage examples:
  ./queuectl.py enqueue '{"id":"job1","command":"sleep 2"}'
  ./queuectl.py worker start --count 3
  ./queuectl.py worker stop
  ./queuectl.py status
  ./queuectl.py list --state pending
  ./queuectl.py dlq list
  ./queuectl.py dlq retry job1
  ./queuectl.py config set max-retries 3
  ./queuectl.py config get max-retries
"""

import argparse
import sqlite3
import os
import sys
import json
import subprocess
import threading
import time
import signal
from datetime import datetime, timedelta
import pathlib

DB_PATH = os.environ.get("QUEUECTL_DB", "queue.db")
PID_FILE = os.environ.get("QUEUECTL_PIDFILE", "queuectl_workers.pid")
DEFAULT_MAX_RETRIES = 3
DEFAULT_BACKOFF_BASE = 2

# Graceful shutdown event (for worker start)
shutdown_event = threading.Event()

def now_ts():
    return int(time.time())

def iso_ts(ts=None):
    if ts is None:
        ts = now_ts()
    return datetime.utcfromtimestamp(ts).isoformat() + "Z"

def init_db(conn):
    c = conn.cursor()
    c.execute("""
    CREATE TABLE IF NOT EXISTS config (
        key TEXT PRIMARY KEY,
        value TEXT NOT NULL
    );
    """)
    c.execute("""
    CREATE TABLE IF NOT EXISTS jobs (
        id TEXT PRIMARY KEY,
        command TEXT NOT NULL,
        state TEXT NOT NULL,
        attempts INTEGER NOT NULL DEFAULT 0,
        max_retries INTEGER NOT NULL,
        next_run_at INTEGER NOT NULL,
        last_error TEXT,
        created_at INTEGER NOT NULL,
        updated_at INTEGER NOT NULL
    );
    """)
    conn.commit()
    # Ensure defaults exist
    set_config_if_missing(conn, "max_retries", str(DEFAULT_MAX_RETRIES))
    set_config_if_missing(conn, "backoff_base", str(DEFAULT_BACKOFF_BASE))
    conn.commit()

def get_conn():
    conn = sqlite3.connect(DB_PATH, timeout=30, isolation_level=None)
    # Use WAL for better concurrency
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.row_factory = sqlite3.Row
    init_db(conn)
    return conn

def set_config_if_missing(conn, key, value):
    c = conn.cursor()
    c.execute("INSERT OR IGNORE INTO config (key, value) VALUES (?, ?)", (key, value))

def config_set(conn, key, value):
    c = conn.cursor()
    c.execute("INSERT INTO config (key, value) VALUES (?, ?) ON CONFLICT(key) DO UPDATE SET value=excluded.value", (key, value))
    conn.commit()

def config_get(conn, key, default=None):
    c = conn.cursor()
    r = c.execute("SELECT value FROM config WHERE key=?",(key,)).fetchone()
    return r["value"] if r else default

def enqueue_job(conn, job_json):
    """
    job_json: dict with at least id and command; optional max_retries
    """
    job_id = job_json.get("id")
    command = job_json.get("command")
    if not job_id or not command:
        raise ValueError("job must have 'id' and 'command'")
    max_retries = int(job_json.get("max_retries", config_get(conn, "max_retries") or DEFAULT_MAX_RETRIES))
    now = now_ts()
    next_run = now
    c = conn.cursor()
    c.execute("""
      INSERT INTO jobs (id, command, state, attempts, max_retries, next_run_at, created_at, updated_at)
      VALUES (?, ?, 'pending', 0, ?, ?, ?, ?)
    """, (job_id, command, max_retries, next_run, now, now))
    conn.commit()

def _try_lock_next_job(conn, worker_name):
    """Return job row if locked, else None. Uses select+update to avoid races."""
    c = conn.cursor()
    now = now_ts()
    # find a pending job eligible to run
    r = c.execute("SELECT id FROM jobs WHERE state='pending' AND next_run_at <= ? ORDER BY created_at LIMIT 1", (now,)).fetchone()
    if not r:
        return None
    job_id = r["id"]
    # try to move to processing only if still pending
    res = c.execute("UPDATE jobs SET state='processing', updated_at=?, last_error=NULL WHERE id=? AND state='pending'", (now, job_id))
    if res.rowcount == 1:
        # successfully locked
        row = c.execute("SELECT * FROM jobs WHERE id=?", (job_id,)).fetchone()
        return row
    return None

def mark_job_completed(conn, job_id):
    now = now_ts()
    c = conn.cursor()
    c.execute("UPDATE jobs SET state='completed', updated_at=? WHERE id=?", (now, job_id))
    conn.commit()

def mark_job_failed_with_retry(conn, job_id, attempts, max_retries, backoff_base, error_text):
    now = now_ts()
    attempts = attempts + 1
    if attempts > max_retries:
        # move to dead
        c = conn.cursor()
        c.execute("UPDATE jobs SET state='dead', attempts=?, last_error=?, updated_at=? WHERE id=?", (attempts, error_text, now, job_id))
        conn.commit()
        return "dead"
    else:
        # exponential backoff: delay = base ** attempts (seconds)
        delay = (backoff_base ** attempts)
        next_run = now + int(delay)
        c = conn.cursor()
        c.execute("UPDATE jobs SET attempts=?, next_run_at=?, last_error=?, state='failed', updated_at=? WHERE id=?", (attempts, next_run, error_text, now, job_id))
        conn.commit()
        return "retry"

def worker_loop(worker_index, poll_interval=1.0):
    conn = get_conn()
    worker_name = f"worker-{os.getpid()}-{worker_index}"
    backoff_base = int(config_get(conn, "backoff_base") or DEFAULT_BACKOFF_BASE)
    print(f"[{worker_name}] started")
    while not shutdown_event.is_set():
        job = None
        try:
            job = _try_lock_next_job(conn, worker_name)
        except Exception as e:
            print(f"[{worker_name}] DB error while grabbing job: {e}")
        if not job:
            time.sleep(poll_interval)
            continue
        # Execute job
        job_id = job["id"]
        command = job["command"]
        print(f"[{worker_name}] executing job {job_id}: {command}")
        try:
            # execute command in shell so that simple commands like "sleep 2" and "echo hi" work
            completed = subprocess.run(command, shell=True)
            exit_code = completed.returncode
            if exit_code == 0:
                mark_job_completed(conn, job_id)
                print(f"[{worker_name}] job {job_id} completed")
            else:
                err = f"exit_code:{exit_code}"
                attempts = job["attempts"]
                max_retries = job["max_retries"]
                res = mark_job_failed_with_retry(conn, job_id, attempts, max_retries, backoff_base, err)
                if res == "dead":
                    print(f"[{worker_name}] job {job_id} moved to DLQ after {attempts+1} attempts (exit {exit_code})")
                else:
                    # compute next_run for display
                    next_run = conn.execute("SELECT next_run_at FROM jobs WHERE id=?", (job_id,)).fetchone()["next_run_at"]
                    print(f"[{worker_name}] job {job_id} will retry at {iso_ts(next_run)} (attempt {attempts+1}/{max_retries})")
        except FileNotFoundError as e:
            # command not found -> treat as failure and retry
            err = f"cmd_not_found:{e}"
            attempts = job["attempts"]
            max_retries = job["max_retries"]
            res = mark_job_failed_with_retry(conn, job_id, attempts, max_retries, backoff_base, err)
            if res == "dead":
                print(f"[{worker_name}] job {job_id} moved to DLQ (command not found)")
            else:
                next_run = conn.execute("SELECT next_run_at FROM jobs WHERE id=?", (job_id,)).fetchone()["next_run_at"]
                print(f"[{worker_name}] job {job_id} will retry at {iso_ts(next_run)} (cmd not found)")
        except Exception as e:
            err = f"exception:{e}"
            attempts = job["attempts"]
            max_retries = job["max_retries"]
            res = mark_job_failed_with_retry(conn, job_id, attempts, max_retries, backoff_base, err)
            if res == "dead":
                print(f"[{worker_name}] job {job_id} moved to DLQ after exception")
            else:
                next_run = conn.execute("SELECT next_run_at FROM jobs WHERE id=?", (job_id,)).fetchone()["next_run_at"]
                print(f"[{worker_name}] job {job_id} will retry at {iso_ts(next_run)} (exception)")

    print(f"[{worker_name}] shutting down (graceful)")

def write_pidfile(pid):
    with open(PID_FILE, "w") as f:
        f.write(str(pid))

def read_pidfile():
    if not os.path.exists(PID_FILE):
        return None
    try:
        with open(PID_FILE, "r") as f:
            pid = int(f.read().strip())
            return pid
    except Exception:
        return None

def remove_pidfile():
    try:
        if os.path.exists(PID_FILE):
            os.remove(PID_FILE)
    except Exception:
        pass

def start_workers(count):
    """
    Starts 'count' worker threads in the current process. This command is intended to be run
    as a long-running process (foreground) that you can background using shell & if desired.
    We write the master PID to PID_FILE so 'worker stop' can signal it.
    """
    # Only allow one master writer at a time (we use PID file)
    if os.path.exists(PID_FILE):
        existing = read_pidfile()
        if existing and _process_is_running(existing):
            print(f"Workers already running under PID {existing}. Stop first or remove {PID_FILE}.")
            return
        else:
            remove_pidfile()

    write_pidfile(os.getpid())
    print(f"[master] starting {count} worker(s) with PID {os.getpid()}. PID file: {PID_FILE}")
    threads = []
    for i in range(count):
        t = threading.Thread(target=worker_loop, args=(i,), daemon=False)
        t.start()
        threads.append(t)

    # handle signals for graceful shutdown
    def _handle(sig, frame):
        print(f"[master] received signal {sig}; initiating graceful shutdown...")
        shutdown_event.set()
    signal.signal(signal.SIGINT, _handle)
    signal.signal(signal.SIGTERM, _handle)

    try:
        # wait for threads
        for t in threads:
            while t.is_alive():
                t.join(timeout=1.0)
    finally:
        shutdown_event.set()
        remove_pidfile()
        print("[master] all workers stopped")

def _process_is_running(pid):
    try:
        os.kill(pid, 0)
        return True
    except OSError:
        return False

def stop_workers():
    pid = read_pidfile()
    if not pid:
        print("No running workers found (pidfile missing).")
        return
    if not _process_is_running(pid):
        print(f"No process with PID {pid} is running. Removing stale pidfile.")
        remove_pidfile()
        return
    print(f"Sending SIGTERM to master PID {pid} to stop workers gracefully.")
    try:
        os.kill(pid, signal.SIGTERM)
    except Exception as e:
        print(f"Could not signal process {pid}: {e}")

def status():
    conn = get_conn()
    c = conn.cursor()
    rows = c.execute("SELECT state, COUNT(*) as n FROM jobs GROUP BY state").fetchall()
    counts = {r["state"]: r["n"] for r in rows}
    print("Jobs summary:")
    for s in ("pending", "processing", "completed", "failed", "dead"):
        print(f"  {s:10s} : {counts.get(s,0)}")
    pid = read_pidfile()
    if pid and _process_is_running(pid):
        print(f"Active worker master PID: {pid}")
    else:
        print("No active worker master process (or stale pidfile)")

def list_jobs(state=None):
    conn = get_conn()
    c = conn.cursor()
    if state:
        rows = c.execute("SELECT * FROM jobs WHERE state=? ORDER BY created_at", (state,)).fetchall()
    else:
        rows = c.execute("SELECT * FROM jobs ORDER BY created_at").fetchall()
    for r in rows:
        print_job_row(r)

def print_job_row(r):
    print(json.dumps({
        "id": r["id"],
        "command": r["command"],
        "state": r["state"],
        "attempts": r["attempts"],
        "max_retries": r["max_retries"],
        "next_run_at": iso_ts(r["next_run_at"]),
        "last_error": r["last_error"],
        "created_at": iso_ts(r["created_at"]),
        "updated_at": iso_ts(r["updated_at"])
    }, indent=2))

def dlq_list():
    conn = get_conn()
    c = conn.cursor()
    rows = c.execute("SELECT * FROM jobs WHERE state='dead' ORDER BY updated_at").fetchall()
    if not rows:
        print("DLQ empty")
        return
    for r in rows:
        print_job_row(r)

def dlq_retry(job_id):
    conn = get_conn()
    c = conn.cursor()
    r = c.execute("SELECT * FROM jobs WHERE id=? AND state='dead'", (job_id,)).fetchone()
    if not r:
        print("No dead job with id", job_id)
        return
    # reset attempts and move to pending
    now = now_ts()
    c.execute("UPDATE jobs SET state='pending', attempts=0, next_run_at=?, last_error=NULL, updated_at=? WHERE id=?", (now, now, job_id))
    conn.commit()
    print("Moved job back to pending:", job_id)

def config_command_set(key, value):
    conn = get_conn()
    config_set(conn, key, value)
    print(f"Set config {key} = {value}")

def config_command_get(key):
    conn = get_conn()
    v = config_get(conn, key)
    if v is None:
        print("Not set")
    else:
        print(v)

def parse_and_run():
    parser = argparse.ArgumentParser(prog="queuectl", description="queuectl - simple job queue CLI")
    sub = parser.add_subparsers(dest="cmd")

    # enqueue
    p_enq = sub.add_parser("enqueue", help="Add a new job to the queue")
    p_enq.add_argument("job_json", help='Job JSON, e.g. \'{"id":"job1","command":"sleep 2"}\'')

    # worker
    p_worker = sub.add_parser("worker", help="Worker management")
    wsub = p_worker.add_subparsers(dest="subcmd")
    p_worker_start = wsub.add_parser("start", help="Start worker(s)")
    p_worker_start.add_argument("--count", "-c", type=int, default=1, help="Number of worker threads to start (in this process)")
    p_worker_stop = wsub.add_parser("stop", help="Stop running workers gracefully")

    # status
    sub.add_parser("status", help="Show summary of jobs & active workers")

    # list
    p_list = sub.add_parser("list", help="List jobs (optionally by state)")
    p_list.add_argument("--state", choices=["pending","processing","completed","failed","dead"], help="Filter by state")

    # dlq
    p_dlq = sub.add_parser("dlq", help="Dead Letter Queue commands")
    dlqsub = p_dlq.add_subparsers(dest="dlqcmd")
    dlqsub.add_parser("list", help="List DLQ jobs")
    dlq_retry = dlqsub.add_parser("retry", help="Retry a job from DLQ")
    dlq_retry.add_argument("job_id", help="Job id to retry")

    # config
    p_cfg = sub.add_parser("config", help="configuration")
    cfgsub = p_cfg.add_subparsers(dest="cfgcmd")
    cfg_set = cfgsub.add_parser("set", help="Set config value")
    cfg_set.add_argument("key")
    cfg_set.add_argument("value")
    cfg_get = cfgsub.add_parser("get", help="Get config value")
    cfg_get.add_argument("key")

    args = parser.parse_args()
    if not args.cmd:
        parser.print_help()
        return

    if args.cmd == "enqueue":
        conn = get_conn()
        try:
            job_json = json.loads(args.job_json)
        except json.JSONDecodeError as e:
            print("Invalid JSON:", e)
            return
        try:
            enqueue_job(conn, job_json)
            print("Enqueued job:", job_json.get("id"))
        except sqlite3.IntegrityError:
            print("Job id already exists!")
        except Exception as e:
            print("Error enqueuing job:", e)

    elif args.cmd == "worker":
        if args.subcmd == "start":
            start_workers(args.count)
        elif args.subcmd == "stop":
            stop_workers()
        else:
            print("worker subcommands: start | stop")
    elif args.cmd == "status":
        status()
    elif args.cmd == "list":
        list_jobs(state=args.state)
    elif args.cmd == "dlq":
        if args.dlqcmd == "list":
            dlq_list()
        elif args.dlqcmd == "retry":
            dlq_retry(args.job_id)
        else:
            print("dlq subcommands: list | retry <job_id>")
    elif args.cmd == "config":
        if args.cfgcmd == "set":
            config_command_set(args.key, args.value)
        elif args.cfgcmd == "get":
            config_command_get(args.key)
        else:
            print("config subcommands: set | get")
    else:
        parser.print_help()

if __name__ == "__main__":
    parse_and_run()

