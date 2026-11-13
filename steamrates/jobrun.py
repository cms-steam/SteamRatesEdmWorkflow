"""Run one counting job of a work dir (what the local executor and batch jobs execute).

    python3 -m steamrates.jobrun WORKDIR/jobs/job_0003.json -o OUTPUT.root [--attempt ATTEMPT.json]

The job file (written by ``prepare``) holds the input files, the ``count`` options and the lumi
mask, so it is self-contained: a batch job only needs it and the code snapshot. The lumi mask is
written to a temporary file for ``count --lumi-json``. The job's ID and key are stored in the
partial result, so ``status`` can tell which job wrote it.

``--attempt`` writes the attempt record (exit code, times, CPU, peak memory) at the end, also when
the counting fails. Batch jobs use it; the local executor writes the record itself.
"""

from __future__ import annotations

import argparse
import datetime
import json
import os
import resource
import socket
import sys
import tempfile
import time
import traceback

from . import __version__, count
from .workdir import JOB_SCHEMA, read_json


def write_attempt(path, job, rc, start, executor, batch_id=None):
    """The attempt record that ``status`` reads (same keys as the local executor's)."""
    usage = [resource.getrusage(w) for w in (resource.RUSAGE_SELF, resource.RUSAGE_CHILDREN)]
    record = {"job": job.get("id"), "key": job.get("key"), "executor": executor, "host": socket.gethostname(),
              "exit_code": rc, "runtime_s": round(time.time() - start, 1),
              "cpu_s": round(sum(u.ru_utime + u.ru_stime for u in usage), 1),
              "max_rss_mb": round(max(u.ru_maxrss for u in usage) / 1024),
              "finished": datetime.datetime.now(datetime.timezone.utc).isoformat(timespec="seconds")}
    if batch_id:
        record["batch_id"] = batch_id
    with open(path, "w") as f:
        json.dump(record, f, indent=1)


def run_job(job, output):
    """Count the job's files into ``output``; returns the exit code of ``count``."""
    with tempfile.TemporaryDirectory(prefix="steamrates_job_") as tmp:
        argv_count = list(job["count_args"]) + ["-o", output,
                                                "--job-info", json.dumps({"id": job["id"], "key": job["key"],
                                                                          "measurement": job["measurement"]})]
        if job.get("lumi_mask") is not None:
            mask_path = os.path.join(tmp, "lumi_mask.json")
            with open(mask_path, "w") as f:
                json.dump(job["lumi_mask"], f)
            argv_count += ["--lumi-json", mask_path]
        argv_count += ["--"] + list(job["files"])
        return count.main(argv_count)


def main(argv=None):
    p = argparse.ArgumentParser(prog="python3 -m steamrates.jobrun", description=__doc__.split("\n\n")[0])
    p.add_argument("job", help="job file (jobs/job_<id>.json)")
    p.add_argument("-o", "--output", required=True, help="partial result to write")
    p.add_argument("--attempt", help="also write the attempt record (JSON) to this file")
    p.add_argument("--executor", default="local", help="executor name for the attempt record")
    p.add_argument("--batch-id", help="batch system job ID for the attempt record")
    args = p.parse_args(argv)

    t0 = time.time()
    job = read_json(args.job)
    if job.get("schema") != JOB_SCHEMA:
        raise SystemExit(f"{args.job}: unknown job schema {job.get('schema')!r}")
    print(f"steamrates job {job['id']} of {job['measurement']}: {len(job['files'])} files, "
          f"{job.get('n_events')} events", flush=True)
    print(f"  host {socket.gethostname()}, {datetime.datetime.now().isoformat(timespec='seconds')}, "
          f"steamrates {__version__} from {os.path.dirname(os.path.abspath(count.__file__))}", flush=True)
    try:
        rc = run_job(job, args.output)
    except SystemExit as e:                     # argparse/usage errors inside count
        rc = e.code if isinstance(e.code, int) else 1
    except Exception:  # noqa: BLE001  (any failure must still leave an attempt record)
        traceback.print_exc()
        rc = 1
    rc = rc or 0
    print(f"steamrates job {job['id']} finished with exit code {rc} after {time.time() - t0:.1f} s", flush=True)
    if args.attempt:
        write_attempt(args.attempt, job, rc, t0, args.executor, args.batch_id)
    return rc


if __name__ == "__main__":
    sys.exit(main())
