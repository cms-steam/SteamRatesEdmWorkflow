"""status: which jobs of a work dir are done, failed or still missing.

    python3 -m steamrates status WORKDIR [--all]

A job is **done** when its partial result exists, can be read, and was written by this job (the
job key stored in the partial matches the job table, i.e. same files, counting options and lumi
mask). Otherwise it is
  * **queued** / **running** / **held**: in the HTCondor queue (only after ``submit --condor``;
    for held jobs condor's hold reason is shown, e.g. memory or time limit exceeded);
  * **invalid**: a partial exists but is unreadable or belongs to another job definition;
  * **failed**:  the last attempt ended without a valid partial (the end of its log is shown);
    a condor job that failed brings back an empty partial;
  * **missing**: no attempt yet.
Every attempt leaves ``logs/job_<id>.log`` and ``logs/job_<id>.attempt.json`` (exit code, times,
executor).
"""

from __future__ import annotations

import argparse
import os
import sys
from concurrent.futures import ThreadPoolExecutor

from . import condor
from .partial import read_metadata
from .workdir import WorkDir, read_json

STATES = ("done", "running", "queued", "held", "failed", "invalid", "missing")


def log_path(wd, job_id):
    return wd.p("logs", WorkDir.job_name(job_id) + ".log")


def attempt_path(wd, job_id):
    return wd.p("logs", WorkDir.job_name(job_id) + ".attempt.json")


def log_tail(path, n=5):
    try:
        with open(path, errors="replace") as f:
            lines = f.read().splitlines()
    except OSError:
        return []
    return lines[-n:]


def job_state(wd, job, queue=None):
    """``(state, detail)`` of one job; ``queue``: condor queue states (``condor.queue_states``)."""
    path = wd.partial(job["id"])
    if os.path.exists(path) and os.path.getsize(path) > 0:
        try:
            meta = read_metadata(path)
        except Exception as e:  # noqa: BLE001  (any unreadable file is "invalid")
            return "invalid", f"unreadable partial ({type(e).__name__}: {e})"
        if (meta.get("job") or {}).get("key") != job["key"]:
            return "invalid", "the partial was written by a different job definition"
        return "done", ""
    if queue and job["id"] in queue:
        state, reason, batch_id = queue[job["id"]]
        return state, f"condor job {batch_id}" + (f": {reason}" if reason else "")
    empty = os.path.exists(path)                # a failed condor job brings back an empty partial
    if os.path.exists(attempt_path(wd, job["id"])):
        attempt = read_json(attempt_path(wd, job["id"]))
        if attempt.get("key") == job["key"]:        # (an attempt of an earlier job definition does not count)
            if attempt.get("exit_code") != 0:
                where = str(attempt.get("executor")) + (f" {attempt['batch_id']}" if attempt.get("batch_id") else "")
                return "failed", f"exit code {attempt.get('exit_code')} ({where}, {attempt.get('host')})"
            if not empty:
                return "missing", "the last attempt succeeded, but its partial is gone"
    if empty:
        return "failed", "empty partial result (the job failed)"
    return "missing", ""


def condor_queue(wd, warn=True):
    """Condor queue states of the work dir's jobs, or None if it never used condor or the query fails."""
    if not condor.submissions(wd):
        return None
    try:
        return condor.queue_states(wd)
    except condor.CondorError as e:
        if warn:
            print(f"warning: cannot query the condor queue ({e}); queued/running jobs show as missing/failed")
        return None


def job_states(wd, threads=8, queue="auto"):
    """``[(job, state, detail), ...]`` for all jobs (partials are checked in parallel).

    ``queue``: condor queue states, or "auto" = ask condor if the work dir has condor submissions."""
    jobs = wd.jobs()
    if queue == "auto":
        queue = condor_queue(wd)
    with ThreadPoolExecutor(threads) as pool:
        states = list(pool.map(lambda j: job_state(wd, j, queue), jobs))
    return [(j, s, d) for j, (s, d) in zip(jobs, states)]


def compact_ids(ids):
    """[0, 1, 2, 5, 7, 8] -> '0-2, 5, 7-8'."""
    ids = sorted(ids)
    parts, start = [], None
    for i, x in enumerate(ids):
        if start is None:
            start = x
        if i + 1 == len(ids) or ids[i + 1] != x + 1:
            parts.append(str(start) if start == x else f"{start}-{x}")
            start = None
    return ", ".join(parts)


def parse_ids(text):
    """'0-2,5' -> {0, 1, 2, 5}."""
    out = set()
    for part in text.split(","):
        part = part.strip()
        if "-" in part:
            a, b = part.split("-", 1)
            out.update(range(int(a), int(b) + 1))
        elif part:
            out.add(int(part))
    return out


def print_status(wd, states, show_all=False, tail=5):
    by_state = {s: [j for j, st, _ in states if st == s] for s in STATES}
    n = len(states)
    print(f"{wd}: {n} jobs  " + "  ".join(f"{s} {len(by_state[s])}" for s in STATES if by_state[s] or s == "done"))
    for job, state, detail in states:
        if state in ("failed", "invalid", "held"):
            print(f"  {WorkDir.job_name(job['id'])} {state}: {detail}")
            if state != "held":
                for line in log_tail(log_path(wd, job["id"]), tail):
                    print(f"      | {line}")
    for state in ("running", "queued", "missing"):
        if by_state[state]:
            print(f"  {state}: {compact_ids(j['id'] for j in by_state[state])}")
    if show_all:
        print(f"  done: {compact_ids(j['id'] for j in by_state['done'])}")
    return by_state


def main(argv=None):
    p = argparse.ArgumentParser(prog="python3 -m steamrates status", description=__doc__.split("\n\n")[0])
    p.add_argument("workdir")
    p.add_argument("--all", action="store_true", help="also list the done jobs")
    p.add_argument("--tail", type=int, default=5, help="log lines shown per failed job (default: %(default)s)")
    args = p.parse_args(argv)
    wd = WorkDir(args.workdir)
    if not wd.is_prepared():
        raise SystemExit(f"error: {wd} is not a prepared work dir (run 'prepare' first)")
    by_state = print_status(wd, job_states(wd), args.all, args.tail)
    return 0 if len(by_state["done"]) == len(wd.jobs()) else 1


if __name__ == "__main__":
    sys.exit(main())
