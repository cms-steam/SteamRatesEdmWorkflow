"""HTCondor executor (lxplus): submit the counting jobs of a work dir and ask the queue about them.

Used by ``submit --condor``, ``status`` and ``run --condor``. One submission writes::

    condor/run_job.sh             job wrapper (the same for every job of the work dir)
    condor/submit_<time>.sub      submit file
    condor/queue_<time>.txt       the jobs of this submission ("<id> job_<id>" per line)
    condor/submissions.json       every submission: time, cluster, job IDs, schedd pool

and each job brings back (condor file transfer, so it works the same from AFS and EOS work dirs)::

    partials/job_<id>.root        the partial result; an EMPTY file if the job failed
    logs/job_<id>.attempt.json    exit code, host, run time, CPU time, memory
    logs/job_<id>.log             the job's output (stdout and stderr)
    logs/condor/                  condor's own log (one per cluster) and the jobs' stderr (normally empty)

A failed job still ends with exit code 0 and an empty partial, so condor does not put it on hold
for a missing output file; ``status`` reports it as failed (with the end of its log) and ``submit``
runs it again. Jobs that condor itself stops (memory or time limit) go on hold: ``status`` shows
the hold reason, and ``submit`` removes held jobs from the queue before it submits them again.

**Work dirs on EOS** need the eossubmit schedds (https://batchdocs.web.cern.ch/local/eossubmit.html):
``module load lxbatch/eossubmit`` before ``condor_submit``/``condor_q``/``condor_rm``. ``module`` is
a shell function, so the environment it sets is taken once from a login shell and passed to the
condor commands. eossubmit rules that the layout above follows: every job file (executable,
transferred inputs, out/err, log) is on EOS; one log per cluster; log directories exist before
submission; outputs come back through file transfer only.

Each job carries two custom ClassAd attributes, ``SteamratesWorkdir`` (absolute work dir path) and
``SteamratesJob`` (job ID), so ``condor_q`` finds the jobs of a work dir without a cluster list.
"""

from __future__ import annotations

import datetime
import os
import re
import shlex
import stat
import subprocess

from .workdir import WorkDir, read_json, write_json

# JobStatus codes of HTCondor -> our job states (others: removed/completed = not in the queue any more)
QUEUE_STATES = {1: "queued", 2: "running", 5: "held", 6: "running", 7: "held"}

SUBMISSIONS = os.path.join("condor", "submissions.json")

RUN_JOB_SH = r"""#!/bin/bash
# steamrates HTCondor job wrapper (written by 'python3 -m steamrates submit --condor').
#     run_job.sh JOB_NAME BATCH_ID CMSSW_BASE
# Runs in the job's scratch dir, where condor put code.tar.gz and JOB_NAME.json (from jobs/).
# It always leaves partial.root and attempt.json for the output transfer: if anything fails,
# partial.root is an empty file, which 'steamrates status' reports as a failed job (instead of a
# job held by condor because an output file is missing). Exit codes of the setup steps:
# 90 = cmsset_default.sh, 91 = CMSSW environment, 92 = code tarball.
exec 2>&1
JOB=$1
BATCH_ID=$2
CMSSW_AREA=$3
echo "steamrates condor job $JOB (condor job $BATCH_ID) on $(hostname) at $(date -Iseconds)"
echo "CMSSW area: $CMSSW_AREA"
rm -f partial.root attempt.json
rc=0
source /cvmfs/cms.cern.ch/cmsset_default.sh || rc=90
if [ $rc -eq 0 ]; then
    { cd "$CMSSW_AREA/src" && eval "$(scramv1 runtime -sh)" && cd - >/dev/null; } || rc=91
fi
if [ $rc -eq 0 ]; then
    tar xzf code.tar.gz || rc=92
fi
if [ $rc -eq 0 ]; then
    PYTHONPATH="$PWD${PYTHONPATH:+:$PYTHONPATH}" python3 -m steamrates.jobrun "$JOB.json" -o partial.root \
        --attempt attempt.json --executor condor --batch-id "$BATCH_ID"
    rc=$?
fi
if [ $rc -ne 0 ]; then
    rm -f partial.root
fi
[ -f partial.root ] || : > partial.root
if [ ! -f attempt.json ]; then
    # jobrun did not get far enough to write it: a minimal record (system python is enough)
    python3 - "$JOB.json" "$rc" "$BATCH_ID" <<'EOF' || echo "{\"exit_code\": $rc, \"executor\": \"condor\"}" > attempt.json
import json, socket, sys
job = json.load(open(sys.argv[1]))
json.dump({"job": job["id"], "key": job["key"], "executor": "condor", "batch_id": sys.argv[3],
           "host": socket.gethostname(), "exit_code": int(sys.argv[2])}, open("attempt.json", "w"))
EOF
fi
echo "steamrates condor job $JOB finished with exit code $rc at $(date -Iseconds)"
exit 0
"""


class CondorError(RuntimeError):
    pass


# ---- environment ------------------------------------------------------------------------------------
def needs_eossubmit(wd):
    """Work dirs on EOS must use the eossubmit schedds."""
    return os.path.realpath(wd.path).startswith("/eos/")


_ENV_CACHE = {}


def condor_env(eossubmit):
    """Environment for the condor commands (``module load lxbatch/eossubmit`` applied if needed)."""
    if not eossubmit:
        return dict(os.environ)
    if "eossubmit" not in _ENV_CACHE:
        try:
            out = subprocess.run(["bash", "-lc", "module load lxbatch/eossubmit >/dev/null 2>&1 && env -0"],
                                 capture_output=True, check=True, timeout=60).stdout
        except (OSError, subprocess.SubprocessError) as e:
            raise CondorError(f"cannot load the lxbatch/eossubmit module: {e}") from None
        env = dict(item.split("=", 1) for item in out.decode(errors="replace").split("\0") if "=" in item)
        if env.get("_myschedd_POOL") != "eossubmit":
            raise CondorError("'module load lxbatch/eossubmit' did not set _myschedd_POOL=eossubmit "
                              "(not on lxplus?); EOS work dirs need it for condor")
        # keep our own environment (e.g. cmsenv), only add what the module sets
        _ENV_CACHE["eossubmit"] = {**os.environ, **{k: v for k, v in env.items()
                                                    if k.startswith(("_condor_", "_myschedd_"))}}
    return dict(_ENV_CACHE["eossubmit"])


def run_condor(cmd, wd, eossubmit=None, timeout=300):
    """Run a condor command for this work dir; returns its stdout (raises CondorError on failure)."""
    eossubmit = needs_eossubmit(wd) if eossubmit is None else eossubmit
    try:
        proc = subprocess.run(cmd, capture_output=True, text=True, cwd=wd.path, env=condor_env(eossubmit),
                              timeout=timeout)
    except (OSError, subprocess.SubprocessError) as e:
        raise CondorError(f"{cmd[0]} failed: {e}") from None
    if proc.returncode != 0:
        raise CondorError(f"{' '.join(shlex.quote(c) for c in cmd[:3])} ... failed (exit code {proc.returncode}):\n"
                          + (proc.stderr or proc.stdout).strip())
    return proc.stdout


# ---- settings -----------------------------------------------------------------------------------------
def find_cmssw_base(start):
    """The CMSSW area that contains ``start`` (the directory with ``.SCRAM``), or None."""
    d = os.path.abspath(start)
    while d != os.path.dirname(d):
        if os.path.isdir(os.path.join(d, ".SCRAM")):
            return d
        d = os.path.dirname(d)
    return None


def cmssw_base(batch):
    """Release for the jobs' environment: ``batch.cmssw_base``, else $CMSSW_BASE, else the CMSSW area
    of this steamrates checkout."""
    base = batch.get("cmssw_base") or os.environ.get("CMSSW_BASE") or find_cmssw_base(os.path.dirname(__file__))
    if not base or not os.path.isdir(os.path.join(base, "src")):
        raise CondorError("no CMSSW area for the jobs: set batch.cmssw_base in the config or run cmsenv")
    return base


_LOCAL_XROOTD = re.compile(r"^root://eos[a-z0-9]*\.cern\.ch/")


def needs_proxy(batch, files):
    """``batch.proxy``: true/false, or auto = inputs read via xrootd from outside CERN EOS."""
    if batch.get("proxy", "auto") != "auto":
        return bool(batch["proxy"])
    return any(f.startswith("root://") and not _LOCAL_XROOTD.match(f) for f in files)


def check_proxy(hours=2):
    try:
        ok = subprocess.run(["voms-proxy-info", "-exists", "-valid", f"{hours}:00"], capture_output=True).returncode == 0
    except OSError:
        ok = False
    if not ok:
        raise CondorError(f"the inputs need a grid proxy valid for at least {hours} h: "
                          "voms-proxy-init -voms cms -valid 192:00")


# ---- submission ---------------------------------------------------------------------------------------
def submit_file_text(wd, queue_file, batch, cmssw_area, proxy, stamp):
    """The submit file of one submission (all paths relative to the work dir = initialdir)."""
    lines = [
        f"# steamrates: counting jobs of the work dir {wd.path}",
        f"# written by 'python3 -m steamrates submit --condor' at {stamp}; the jobs are listed in {queue_file}",
        "universe                = vanilla",
        f"initialdir              = {wd.path}",
        f"executable              = {wd.p('condor', 'run_job.sh')}",
        f"arguments               = $(job_name) $(ClusterId).$(ProcId) {cmssw_area}",
        "should_transfer_files   = YES",
        "when_to_transfer_output = ON_EXIT",
        "transfer_input_files    = code.tar.gz, jobs/$(job_name).json",
        "transfer_output_files   = partial.root, attempt.json",
        'transfer_output_remaps  = "partial.root = partials/$(job_name).root; '
        'attempt.json = logs/$(job_name).attempt.json"',
        "output                  = logs/$(job_name).log",
        "error                   = logs/condor/$(job_name).$(ClusterId).err",
        "log                     = logs/condor/cluster_$(ClusterId).log",
        f'+JobFlavour             = "{batch.get("flavour", "espresso")}"',
        f"request_memory          = {int(batch.get('request_memory_mb', 2000))}",
        'MY.WantOS               = "el9"',
        f'MY.SteamratesWorkdir    = "{wd.path}"',
        "MY.SteamratesJob        = $(job_id)",
    ]
    if proxy:
        lines.append("use_x509userproxy       = true")
    lines.append(f"queue job_id, job_name from {queue_file}")
    return "\n".join(lines) + "\n"


def write_submission(wd, jobs, batch, cmssw_area, proxy):
    """Write run_job.sh, the queue file and the submit file; returns (submit file, stamp)."""
    os.makedirs(wd.p("condor"), exist_ok=True)
    os.makedirs(wd.p("logs", "condor"), exist_ok=True)     # eossubmit: log directories must exist
    script = wd.p("condor", "run_job.sh")
    with open(script, "w") as f:
        f.write(RUN_JOB_SH)
    os.chmod(script, os.stat(script).st_mode | stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH)
    now = datetime.datetime.now()
    stamp = now.strftime("%Y%m%d_%H%M%S")
    queue_file = os.path.join("condor", f"queue_{stamp}.txt")
    with open(wd.p(queue_file), "w") as f:
        for job in jobs:
            f.write(f"{job['id']} {WorkDir.job_name(job['id'])}\n")
    sub = os.path.join("condor", f"submit_{stamp}.sub")
    with open(wd.p(sub), "w") as f:
        f.write(submit_file_text(wd, queue_file, batch, cmssw_area, proxy, now.isoformat(timespec="seconds")))
    return sub, stamp


_SUBMITTED = re.compile(r"(\d+) job\(s\) submitted to cluster (\d+)")


def submit_jobs(wd, jobs, batch, dry_run=False):
    """Submit ``jobs`` (job dicts) to condor. Returns the submission record (cluster None for a dry run)."""
    files = [f for job in jobs for f in job["files"]]
    proxy = needs_proxy(batch, files)
    if proxy and not dry_run:
        check_proxy()
    area = cmssw_base(batch)
    # an empty partial of an earlier failed attempt must not be mistaken for a result while the job waits
    for job in jobs:
        path = wd.partial(job["id"])
        if os.path.exists(path) and os.path.getsize(path) == 0:
            os.remove(path)
    sub, stamp = write_submission(wd, jobs, batch, area, proxy)
    eos = needs_eossubmit(wd)
    record = {"time": datetime.datetime.now(datetime.timezone.utc).isoformat(timespec="seconds"),
              "submit_file": sub, "cluster": None, "jobs": [j["id"] for j in jobs],
              "schedd_pool": "eossubmit" if eos else "default", "cmssw_base": area, "proxy": proxy,
              "flavour": batch.get("flavour"), "request_memory_mb": batch.get("request_memory_mb")}
    if dry_run:
        return record
    out = run_condor(["condor_submit", sub], wd, eos)
    m = _SUBMITTED.search(out)
    if not m:
        raise CondorError(f"cannot find the cluster ID in the condor_submit output:\n{out}")
    record["cluster"] = int(m.group(2))
    if int(m.group(1)) != len(jobs):
        raise CondorError(f"condor_submit submitted {m.group(1)} jobs instead of {len(jobs)}:\n{out}")
    path = wd.p(SUBMISSIONS)
    history = read_json(path) if os.path.exists(path) else []
    write_json(path, history + [record])
    return record


def submissions(wd):
    path = wd.p(SUBMISSIONS)
    return read_json(path) if os.path.exists(path) else []


# ---- queue ----------------------------------------------------------------------------------------------
def parse_queue(text):
    """Lines of ``condor_q -af SteamratesJob JobStatus ClusterId ProcId HoldReason`` ->
    ``{job_id: (state, detail, "cluster.proc")}``. For a job in the queue twice, the newest wins."""
    out, newest = {}, {}
    for line in text.splitlines():
        parts = line.split(None, 4)
        if len(parts) < 4 or not parts[0].isdigit():
            continue
        job_id, status, cluster, proc = int(parts[0]), int(parts[1]), int(parts[2]), int(parts[3])
        state = QUEUE_STATES.get(status)
        if state is None or (job_id in newest and newest[job_id] > (cluster, proc)):
            continue
        reason = parts[4].strip() if len(parts) > 4 and parts[4].strip() != "undefined" else ""
        newest[job_id] = (cluster, proc)
        out[job_id] = (state, reason, f"{cluster}.{proc}")
    return out


def queue_states(wd):
    """Jobs of this work dir that are in the condor queue: ``{job_id: (state, detail, batch_id)}``."""
    out = run_condor(["condor_q", "-constraint", f'SteamratesWorkdir == "{wd.path}"',
                      "-af", "SteamratesJob", "JobStatus", "ClusterId", "ProcId", "HoldReason"], wd, timeout=120)
    return parse_queue(out)


def remove_jobs(wd, batch_ids):
    """condor_rm the given ``cluster.proc`` IDs."""
    if batch_ids:
        run_condor(["condor_rm", *batch_ids], wd)
