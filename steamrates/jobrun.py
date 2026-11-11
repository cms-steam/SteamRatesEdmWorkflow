"""Run one counting job of a work dir (what the local executor and batch jobs execute).

    python3 -m steamrates.jobrun WORKDIR/jobs/job_0003.json -o OUTPUT.root

The job file (written by ``prepare``) holds the input files, the ``count`` options and the lumi
mask, so it is self-contained: a batch job only needs it and the code snapshot. The lumi mask is
written to a temporary file for ``count --lumi-json``. The job's ID and key are stored in the
partial result, so ``status`` can tell which job wrote it.
"""

from __future__ import annotations

import argparse
import datetime
import json
import os
import socket
import sys
import tempfile
import time

from . import __version__, count
from .workdir import JOB_SCHEMA, read_json


def main(argv=None):
    p = argparse.ArgumentParser(prog="python3 -m steamrates.jobrun", description=__doc__.split("\n\n")[0])
    p.add_argument("job", help="job file (jobs/job_<id>.json)")
    p.add_argument("-o", "--output", required=True, help="partial result to write")
    args = p.parse_args(argv)

    job = read_json(args.job)
    if job.get("schema") != JOB_SCHEMA:
        raise SystemExit(f"{args.job}: unknown job schema {job.get('schema')!r}")
    print(f"steamrates job {job['id']} of {job['measurement']}: {len(job['files'])} files, "
          f"{job.get('n_events')} events", flush=True)
    print(f"  host {socket.gethostname()}, {datetime.datetime.now().isoformat(timespec='seconds')}, "
          f"steamrates {__version__} from {os.path.dirname(os.path.abspath(count.__file__))}", flush=True)
    t0 = time.time()
    with tempfile.TemporaryDirectory(prefix="steamrates_job_") as tmp:
        argv_count = list(job["count_args"]) + ["-o", args.output,
                                                "--job-info", json.dumps({"id": job["id"], "key": job["key"],
                                                                          "measurement": job["measurement"]})]
        if job.get("lumi_mask") is not None:
            mask_path = os.path.join(tmp, "lumi_mask.json")
            with open(mask_path, "w") as f:
                json.dump(job["lumi_mask"], f)
            argv_count += ["--lumi-json", mask_path]
        argv_count += ["--"] + list(job["files"])
        rc = count.main(argv_count)
    print(f"steamrates job {job['id']} finished with exit code {rc} after {time.time() - t0:.1f} s", flush=True)
    return rc


if __name__ == "__main__":
    sys.exit(main())
