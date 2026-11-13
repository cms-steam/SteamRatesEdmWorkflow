"""submit: run the counting jobs of a work dir that are not done yet.

    python3 -m steamrates submit WORKDIR --local [-j 8] [--jobs 0-3,7] [--rerun-done]
    python3 -m steamrates submit WORKDIR --condor [--jobs 0-3,7] [--rerun-done] [--dry-run] [--wait]

Only jobs without a valid partial result are run (see ``status``), so ``submit`` can simply be
repeated after failures. Jobs that are still queued or running in HTCondor are skipped; held jobs
are removed from the queue and submitted again. Each job runs ``python3 -m steamrates.jobrun``
with the code snapshot of the work dir (``code/``, ``code.tar.gz``), so editing the checkout while
jobs run does not affect them.

Local executor: up to ``-j`` jobs run in parallel as separate processes (one per job; the work is
CPU- and I/O-bound in numpy/uproot, so processes, not threads, do the counting). The output of a
job goes to ``logs/job_<id>.log``; the attempt (exit code, times) to ``logs/job_<id>.attempt.json``.

HTCondor executor (lxplus; steamrates.condor): one cluster per ``submit``, settings from the
config's ``batch`` section (flavour, memory, proxy, CMSSW area). Work dirs on EOS are submitted
through the eossubmit schedds automatically. The jobs' outputs come back by condor file transfer
to the same places as with the local executor. ``--wait`` waits until no job is queued or running
any more (``status`` shows the progress at any time).
"""

from __future__ import annotations

import argparse
import datetime
import os
import socket
import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

from . import condor
from .jobstatus import STATES, attempt_path, compact_ids, condor_queue, job_states, log_path, log_tail, parse_ids, \
    print_status
from .workdir import WorkDir, write_json


def default_parallel():
    return max(1, min(8, (os.cpu_count() or 2) // 2))


def run_local(wd, jobs, n_parallel, python=sys.executable):
    """Run ``jobs`` (job dicts) locally, ``n_parallel`` at a time. Returns ``{job_id: exit_code}``."""
    env = dict(os.environ)
    env["PYTHONPATH"] = wd.code_dir + (os.pathsep + env["PYTHONPATH"] if env.get("PYTHONPATH") else "")

    def one(job):
        start = time.time()
        with open(log_path(wd, job["id"]), "w") as log:
            proc = subprocess.Popen([python, "-m", "steamrates.jobrun", wd.job_json(job["id"]),
                                     "-o", wd.partial(job["id"])],
                                    stdout=log, stderr=subprocess.STDOUT, env=env, cwd=wd.path)
            # wait4 instead of wait: also gives the resource usage of this child (peak memory)
            _, status, usage = os.wait4(proc.pid, 0)
            proc.returncode = os.waitstatus_to_exitcode(status)
        write_json(attempt_path(wd, job["id"]), {
            "job": job["id"], "key": job["key"], "executor": "local", "host": socket.gethostname(),
            "exit_code": proc.returncode, "runtime_s": round(time.time() - start, 1),
            "cpu_s": round(usage.ru_utime + usage.ru_stime, 1), "max_rss_mb": round(usage.ru_maxrss / 1024),
            "finished": datetime.datetime.now(datetime.timezone.utc).isoformat(timespec="seconds")})
        return job, proc.returncode, time.time() - start, usage.ru_maxrss / 1024

    results = {}
    t0 = time.time()
    with ThreadPoolExecutor(n_parallel) as pool:
        futures = [pool.submit(one, job) for job in jobs]
        try:
            for i, fut in enumerate(as_completed(futures), 1):
                job, rc, dt, rss = fut.result()
                results[job["id"]] = rc
                status = "ok" if rc == 0 else f"FAILED (exit code {rc})"
                print(f"[{i}/{len(jobs)}] {WorkDir.job_name(job['id'])} {status} in {dt:.0f} s, "
                      f"max RSS {rss:.0f} MB (elapsed {time.time() - t0:.0f} s)", flush=True)
                if rc != 0:
                    for line in log_tail(log_path(wd, job["id"]), 3):
                        print(f"      | {line}", flush=True)
        except KeyboardInterrupt:
            for fut in futures:
                fut.cancel()
            raise
    return results


def submit(wd, local=True, n_parallel=None, only=None, rerun_done=False, dry_run=False):
    """Run the jobs that are not done (or all, with ``rerun_done``): locally (waits for them) or on
    HTCondor (returns after the submission). Returns the job states after."""
    queue = condor_queue(wd)
    states = job_states(wd, queue=queue)
    busy = {job["id"] for job, state, _ in states if state in ("queued", "running")}
    todo = [job for job, state, _ in states
            if job["id"] not in busy and (rerun_done or state != "done")]
    if only is not None:
        todo = [job for job in todo if job["id"] in only]
    if busy:
        print(f"{wd}: skipping {len(busy)} job(s) still queued or running in condor: {compact_ids(busy)}")
    if not todo:
        print(f"{wd}: nothing to run ({sum(s == 'done' for _, s, _ in states)}/{len(states)} jobs done)")
        return states
    held = [queue[job["id"]][2] for job in todo if queue and queue.get(job["id"], ("",))[0] == "held"]
    if held and not dry_run:
        print(f"removing {len(held)} held job(s) from the condor queue before submitting them again")
        condor.remove_jobs(wd, held)
    if not local:
        batch = wd.resolved()["settings"]["batch"]
        try:
            record = condor.submit_jobs(wd, todo, batch, dry_run=dry_run)
        except condor.CondorError as e:
            raise SystemExit(f"error: {e}") from None
        if dry_run:
            print(f"{wd}: dry run, wrote {record['submit_file']} for {len(todo)} job(s) (not submitted)")
            return states
        print(f"{wd}: submitted {len(todo)} of {len(states)} jobs to condor cluster {record['cluster']} "
              f"(schedd pool {record['schedd_pool']}, flavour {record['flavour']}, "
              f"{record['request_memory_mb']} MB, CMSSW {record['cmssw_base']})")
        print(f"  progress: python3 -m steamrates status {wd}")
        return job_states(wd)
    n_parallel = n_parallel or default_parallel()
    print(f"{wd}: running {len(todo)} of {len(states)} jobs locally, {n_parallel} at a time", flush=True)
    run_local(wd, todo, n_parallel)
    states = job_states(wd, queue=queue)
    print_status(wd, states, tail=3)
    return states


def wait_condor(wd, poll_s=60, grace_s=180):
    """Wait until none of the work dir's jobs is queued or running in condor; returns the job states.

    The outputs of a finished job can appear on an EOS fuse mount a little after the job left the
    queue (seen on lxplus: ~10 s), so jobs that were in the queue and now look "missing" are
    checked again for up to ``grace_s`` seconds before giving up on them."""
    last, seen, grace_until = None, set(), None
    while True:
        states = job_states(wd)
        seen |= {job["id"] for job, st, _ in states if st in ("queued", "running")}
        counts = {s: sum(st == s for _, st, _ in states) for s in STATES}
        line = "  ".join(f"{s} {n}" for s, n in counts.items() if n or s == "done")
        if line != last:
            print(f"{datetime.datetime.now():%H:%M:%S} {line}", flush=True)
            last = line
        if not counts["queued"] and not counts["running"]:
            arriving = [job["id"] for job, st, _ in states if st == "missing" and job["id"] in seen]
            if not arriving:
                return states
            grace_until = grace_until or time.time() + grace_s
            if time.time() > grace_until:
                print(f"no output arrived for {len(arriving)} job(s) that left the queue: {compact_ids(arriving)}")
                return states
            time.sleep(min(15, poll_s))
            continue
        time.sleep(poll_s)


def main(argv=None):
    p = argparse.ArgumentParser(prog="python3 -m steamrates submit", description=__doc__.split("\n\n")[0],
                                formatter_class=argparse.RawDescriptionHelpFormatter,
                                epilog=__doc__.split("\n\n", 1)[1])
    p.add_argument("workdir")
    where = p.add_mutually_exclusive_group(required=True)
    where.add_argument("--local", action="store_true", help="run on this machine")
    where.add_argument("--condor", action="store_true", help="submit to HTCondor (lxplus)")
    p.add_argument("-j", "--parallel", type=int, help=f"local jobs at a time (default: {default_parallel()})")
    p.add_argument("--jobs", help="only these job IDs, e.g. 0-3,7")
    p.add_argument("--rerun-done", action="store_true", help="also rerun jobs that are done")
    p.add_argument("--dry-run", action="store_true", help="condor: write the submit files, do not submit")
    p.add_argument("--wait", action="store_true", help="condor: wait until no job is queued or running")
    p.add_argument("--poll", type=int, default=60, help="condor --wait: seconds between queue checks (default: 60)")
    args = p.parse_args(argv)
    wd = WorkDir(args.workdir)
    if not wd.is_prepared():
        raise SystemExit(f"error: {wd} is not a prepared work dir (run 'prepare' first)")
    states = submit(wd, local=args.local, n_parallel=args.parallel,
                    only=parse_ids(args.jobs) if args.jobs else None, rerun_done=args.rerun_done,
                    dry_run=args.dry_run)
    if args.condor and args.wait and not args.dry_run:
        states = wait_condor(wd, args.poll)
        print_status(wd, states, tail=3)
    if args.condor and not args.wait:
        return 0
    return 0 if all(s == "done" for _, s, _ in states) else 1


if __name__ == "__main__":
    sys.exit(main())
