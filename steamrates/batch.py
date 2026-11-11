"""submit: run the counting jobs of a work dir that are not done yet.

    python3 -m steamrates submit WORKDIR --local [-j 8] [--jobs 0-3,7] [--rerun-done]
    python3 -m steamrates submit WORKDIR --condor          (lxplus HTCondor; not implemented yet)

Only jobs without a valid partial result are run (see ``status``), so ``submit`` can simply be
repeated after failures. Each job runs ``python3 -m steamrates.jobrun`` with the code snapshot of
the work dir (``code/``), so editing the checkout while jobs run does not affect them.

Local executor: up to ``-j`` jobs run in parallel as separate processes (one per job; the work is
CPU- and I/O-bound in numpy/uproot, so processes, not threads, do the counting). The output of a
job goes to ``logs/job_<id>.log``; the attempt (exit code, times) to ``logs/job_<id>.attempt.json``.
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

from .jobstatus import attempt_path, job_states, log_path, log_tail, parse_ids, print_status
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


def submit(wd, local=True, n_parallel=None, only=None, rerun_done=False):
    """Run the jobs that are not done (or all, with ``rerun_done``). Returns the job states after."""
    states = job_states(wd)
    todo = [job for job, state, _ in states if rerun_done or state != "done"]
    if only is not None:
        todo = [job for job in todo if job["id"] in only]
    if not todo:
        print(f"{wd}: nothing to run ({sum(s == 'done' for _, s, _ in states)}/{len(states)} jobs done)")
        return states
    if not local:
        raise SystemExit("error: --condor is not implemented yet; use --local")
    n_parallel = n_parallel or default_parallel()
    print(f"{wd}: running {len(todo)} of {len(states)} jobs locally, {n_parallel} at a time", flush=True)
    run_local(wd, todo, n_parallel)
    states = job_states(wd)
    print_status(wd, states, tail=3)
    return states


def main(argv=None):
    p = argparse.ArgumentParser(prog="python3 -m steamrates submit", description=__doc__.split("\n\n")[0],
                                formatter_class=argparse.RawDescriptionHelpFormatter,
                                epilog=__doc__.split("\n\n", 1)[1])
    p.add_argument("workdir")
    where = p.add_mutually_exclusive_group(required=True)
    where.add_argument("--local", action="store_true", help="run on this machine")
    where.add_argument("--condor", action="store_true", help="submit to HTCondor (not implemented yet)")
    p.add_argument("-j", "--parallel", type=int, help=f"local jobs at a time (default: {default_parallel()})")
    p.add_argument("--jobs", help="only these job IDs, e.g. 0-3,7")
    p.add_argument("--rerun-done", action="store_true", help="also rerun jobs that are done")
    args = p.parse_args(argv)
    wd = WorkDir(args.workdir)
    if not wd.is_prepared():
        raise SystemExit(f"error: {wd} is not a prepared work dir (run 'prepare' first)")
    states = submit(wd, local=args.local, n_parallel=args.parallel,
                    only=parse_ids(args.jobs) if args.jobs else None, rerun_done=args.rerun_done)
    return 0 if all(s == "done" for _, s, _ in states) else 1


if __name__ == "__main__":
    sys.exit(main())
