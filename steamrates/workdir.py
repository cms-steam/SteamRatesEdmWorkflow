"""The working directory of one measurement, and how its input files are split into jobs.

Everything of a measurement lives below its work dir, so several measurements can run at the same
time from one checkout::

    config.yaml               verbatim copy of the measurement config
    config.resolved.json      every setting filled in, the resolved catalog entries, git state, sha1s
    code/steamrates/          snapshot of the package used by the jobs (immune to edits of the
    code.tar.gz               checkout while jobs run); the tarball is what batch jobs unpack
    inputs/files.json         input files after expansion and duplicate removal (+ per-file checks)
    jobs/jobs.json            job table: id, files, number of events, output, key
    jobs/job_<id>.json        everything one counting job needs (read by steamrates.jobrun)
    partials/job_<id>.root    one partial result per job
    logs/                     job logs
    condor/                   submit files and the list of submitted clusters
    results/                  normalized rates (merge)

A job's ``key`` is the sha1 of what it counts (files, counting options, lumi mask). If the work dir
is prepared again with changed settings, partials of jobs whose key did not change are kept.
"""

from __future__ import annotations

import json
import os

JOB_SCHEMA = "steamrates.job/1"
WORKDIR_SCHEMA = "steamrates.workdir/1"


def write_json(path, obj):
    """Write JSON atomically (temporary file + rename), so readers never see half a file."""
    tmp = f"{path}.tmp-{os.getpid()}"
    with open(tmp, "w") as f:
        json.dump(obj, f, indent=1)
        f.write("\n")
    os.replace(tmp, path)


def read_json(path):
    with open(path) as f:
        return json.load(f)


class WorkDir:
    def __init__(self, path):
        self.path = os.path.abspath(path)

    def __str__(self):
        return self.path

    def p(self, *parts):
        return os.path.join(self.path, *parts)

    # ---- fixed locations --------------------------------------------------------------------------
    @property
    def config_copy(self):
        return self.p("config.yaml")

    @property
    def resolved_json(self):
        return self.p("config.resolved.json")

    @property
    def code_dir(self):
        return self.p("code")

    @property
    def code_tar(self):
        return self.p("code.tar.gz")

    @property
    def files_json(self):
        return self.p("inputs", "files.json")

    @property
    def jobs_json(self):
        return self.p("jobs", "jobs.json")

    @staticmethod
    def job_name(job_id):
        return f"job_{job_id:04d}"

    def job_json(self, job_id):
        return self.p("jobs", self.job_name(job_id) + ".json")

    @staticmethod
    def partial_relpath(job_id):
        return os.path.join("partials", WorkDir.job_name(job_id) + ".root")

    def partial(self, job_id):
        return self.p(self.partial_relpath(job_id))

    def make_dirs(self):
        for d in ("inputs", "jobs", "partials", "logs", "condor", "results"):
            os.makedirs(self.p(d), exist_ok=True)

    # ---- contents ---------------------------------------------------------------------------------
    def is_prepared(self):
        return os.path.exists(self.resolved_json) and os.path.exists(self.jobs_json)

    def resolved(self):
        return read_json(self.resolved_json)

    def jobs(self):
        return read_json(self.jobs_json)["jobs"]

    def files(self):
        return read_json(self.files_json)


def split_jobs(n_events, events_per_job=None, files_per_job=None):
    """Group consecutive files into jobs; returns a list of lists of file indices.

    * ``files_per_job``: fixed number of files per job.
    * ``events_per_job`` (needs ``n_events``, one entry per file): files are added to a job in
      order until the next file would exceed the target; a file larger than the target is a job on
      its own. Files without events are added to the current job (they still carry lumi sections).
    """
    n_files = len(n_events)
    if files_per_job:
        return [list(range(i, min(i + files_per_job, n_files))) for i in range(0, n_files, files_per_job)]
    if not events_per_job:
        raise ValueError("give events_per_job or files_per_job")
    if any(n is None for n in n_events):
        raise ValueError("events_per_job needs the number of events of every file")
    jobs, current, total = [], [], 0
    for i, n in enumerate(n_events):
        if current and total + n > events_per_job and n > 0:
            jobs.append(current)
            current, total = [], 0
        current.append(i)
        total += n
    if current:
        jobs.append(current)
    return jobs
