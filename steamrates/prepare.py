"""prepare: measurement config -> work dir with one job file per counting job.

    python3 -m steamrates prepare meas.yaml -w WORKDIR [--force] [--catalog DIR] [-j THREADS]

Steps:
  1. read and validate the config, resolve the input against the sample catalog (steamrates.config);
  2. expand the inputs (globs, list files), drop duplicates with a warning;
  3. open every file (in parallel) to read its number of events, format and runs; stop on files
     that cannot be read; check the runs against the RAW entry (data);
  4. read the trigger bits of the first file with the counting options, to catch a wrong process
     name before any job runs, and compare its HLT menu with the catalog entry;
  5. split the files into jobs and write the work dir (layout: steamrates.workdir).

Nothing is counted yet. Running ``prepare`` again on the same work dir does nothing if neither the
settings nor the input files changed. If they changed, it stops unless ``--force`` is given; with
``--force``, the work dir is updated, files checked before are not opened again, and the partial
results of jobs that did not change are kept (others are moved to ``partials/stale_<time>/``).
"""

from __future__ import annotations

import argparse
import datetime
import glob
import hashlib
import json
import logging
import os
import shutil
import sys
import tarfile

from . import __version__
from .config import load_measurement
from .inputs import InputError, check_files, expand_inputs
from .provenance import git_state
from .readers import detect_format, make_reader
from .schema import ConfigError
from .workdir import JOB_SCHEMA, WORKDIR_SCHEMA, WorkDir, split_jobs, write_json

log = logging.getLogger("steamrates.prepare")


def count_args(meas, fmt, process, l1_process):
    """Options of ``steamrates.count`` shared by all jobs (files, lumi mask and output are added
    per job by ``steamrates.jobrun``)."""
    c = meas.counting
    args = ["--format", fmt, "--sample", meas.name, "--chunk-size", str(c["chunk_size"])]
    if process:
        args += ["--process", process]
    if l1_process:
        args += ["--l1-process", l1_process]
    if c["weight_branch"]:
        args += ["--weight-branch", c["weight_branch"]]
    if fmt == "nano":
        args += ["--bits-include", c["bits_include"], "--bits-exclude", c["bits_exclude"]]
    if c["allow_duplicate_events"]:
        args.append("--allow-duplicate-events")
    return args


def job_key(files, args, lumi_mask):
    """sha1 of what a job counts. ``--chunk-size`` only changes the memory use, not the result, so
    it is left out: changing it does not invalidate finished jobs."""
    args = list(args)
    if "--chunk-size" in args:
        i = args.index("--chunk-size")
        del args[i:i + 2]
    return hashlib.sha1(json.dumps([files, args, lumi_mask], sort_keys=True).encode()).hexdigest()


def snapshot_code(wd):
    """Copy the steamrates package into the work dir (and a tarball of it for batch jobs)."""
    package = os.path.dirname(os.path.abspath(__file__))
    target = os.path.join(wd.code_dir, "steamrates")
    shutil.rmtree(wd.code_dir, ignore_errors=True)
    shutil.copytree(package, target, ignore=shutil.ignore_patterns("__pycache__", "*.pyc", "*.tmp-*"))
    tmp = f"{wd.code_tar}.tmp-{os.getpid()}"
    with tarfile.open(tmp, "w:gz") as tar:
        tar.add(target, arcname="steamrates")
    os.replace(tmp, wd.code_tar)


def _changed_settings(old, new):
    new = json.loads(json.dumps(new))          # same types as the stored version
    return sorted(k for k in set(old) | set(new) if old.get(k) != new.get(k))


def _check_inputs(meas, files, reuse, threads):
    """Per-file checks (number of events, format, runs), reusing earlier checks of the same files."""
    todo = [p for p in files if p not in reuse]
    if todo:
        log.info("opening %d input files to read their size and lumi sections (%d threads)", len(todo), threads)
    fresh = {c.path: c.to_dict() for c in check_files(todo, threads)}
    checks = [reuse.get(p) or fresh[p] for p in files]
    bad = [c for c in checks if "error" in c]
    if bad:
        lines = "\n".join(f"  {c['path']}: {c['error']}" for c in bad[:10])
        raise ConfigError(f"{len(bad)} of {len(files)} input files cannot be read:\n{lines}"
                          + ("\n  ..." if len(bad) > 10 else ""))
    return checks


def _check_first_file(meas, fmt, path):
    """Open one file with the counting options; returns (process, l1_process, menu name)."""
    c = meas.counting
    reader = make_reader(fmt, process=c["process"], l1_process=c["l1_process"], weight_branch=c["weight_branch"],
                         bits_include=c["bits_include"], bits_exclude=c["bits_exclude"], chunk_size=c["chunk_size"])
    try:
        info = reader.open(path)
    except Exception as e:  # noqa: BLE001  (report any reader error as a config problem)
        raise ConfigError(f"cannot read the trigger bits of {path} with the counting options: {e}") from None
    if fmt == "nano":
        process = reader.selected_processes.get("hlt")
        l1_process = reader.selected_processes.get("l1")
    else:
        process, l1_process = (info.menu.process if info.menu else c["process"]), None
    menu = info.menu.name if info.menu else None
    expected = meas.ntuples.hlt_menu
    if expected and menu and expected != menu:
        log.warning("the HLT menu of %s is %s, but the ntuples entry says %s", path, menu, expected)
    return process, l1_process, menu


def _reuse_partials(wd, jobs):
    """Keep the partials of jobs whose key did not change (renamed to their new IDs); move all
    other partials to ``partials/stale_<time>/``. Nothing is deleted."""
    present = glob.glob(wd.p("partials", "job_*.root"))
    if not present:
        return
    try:
        old_jobs = wd.jobs()
    except (OSError, ValueError, KeyError):
        old_jobs = []
    by_key = {j["key"]: wd.partial(j["id"]) for j in old_jobs if os.path.exists(wd.partial(j["id"]))}
    # 1. every existing partial gets a temporary name first, so renames cannot overwrite each other
    for path in present:
        os.replace(path, path + ".old")
    # 2. partials of unchanged jobs go to the job's (possibly new) ID
    n_kept = 0
    for job in jobs:
        src = by_key.pop(job["key"], None)
        if src is not None:
            os.replace(src + ".old", wd.partial(job["id"]))
            n_kept += 1
    # 3. the rest is moved aside
    leftovers = glob.glob(wd.p("partials", "job_*.root.old"))
    stale_dir = wd.p("partials", "stale_" + datetime.datetime.now().strftime("%Y%m%d_%H%M%S"))
    for path in leftovers:
        os.makedirs(stale_dir, exist_ok=True)
        shutil.move(path, os.path.join(stale_dir, os.path.basename(path)[:-len(".old")]))
    log.info("kept %d partial result(s) of unchanged jobs; moved %d other(s) to %s", n_kept, len(leftovers),
             os.path.relpath(stale_dir, wd.path) if leftovers else "-")


def prepare(config_path, workdir, force=False, catalog_dir=None, threads=None):
    """Prepare (or update) a work dir. Returns ``(WorkDir, changed)``."""
    meas = load_measurement(config_path, catalog_dir)
    for w in meas.warnings:
        log.warning(w)
    wd = WorkDir(workdir)
    settings = meas.settings()
    settings_sha1 = meas.settings_sha1()

    # ---- inputs -----------------------------------------------------------------------------------
    inputs = expand_inputs(meas.ntuples.inputs, meas.ntuples.base_dir)
    if inputs.duplicates:
        examples = "; ".join(f"{b} (same as {a})" for a, b in inputs.duplicates[:3])
        log.warning("dropped %d duplicate input file(s), e.g. %s", len(inputs.duplicates), examples)

    # ---- existing work dir ------------------------------------------------------------------------
    reuse = {}
    if wd.is_prepared():
        old = wd.resolved()
        old_files = [f["path"] for f in wd.files()["files"]]
        changed = _changed_settings(old["settings"], settings)
        new_files = sorted(set(inputs.files) - set(old_files))
        gone_files = sorted(set(old_files) - set(inputs.files))
        if not changed and old_files == inputs.files:
            log.info("%s is up to date (same settings and input files); nothing to do", wd)
            return wd, False
        what = []
        if changed:
            what.append("settings changed: " + ", ".join(changed))
        if old_files != inputs.files:
            what.append(f"input files changed: {len(new_files)} new, {len(gone_files)} gone")
        if not force:
            raise ConfigError(f"{wd} was prepared before and the measurement changed ({'; '.join(what)}). "
                              "Use --force to update it (partial results of unchanged jobs are kept), "
                              "or use a new work dir.")
        log.warning("updating %s: %s", wd, "; ".join(what))
        reuse = {f["path"]: f for f in wd.files()["files"] if "n_events" in f}

    # ---- per-file checks --------------------------------------------------------------------------
    checks = None
    if meas.jobs["check_inputs"]:
        checks = _check_inputs(meas, inputs.files, reuse, threads or meas.jobs["check_threads"])
        formats = sorted({c["format"] for c in checks})
        if len(formats) > 1:
            raise ConfigError(f"the input files have different formats: {formats}")
        fmt = formats[0]
        if meas.counting["format"] and meas.counting["format"] != fmt:
            raise ConfigError(f"the input files are {fmt!r}, but the format is set to {meas.counting['format']!r}")
        if meas.is_data:
            runs = sorted({r for c in checks for r in c["runs"]})
            known = meas.raw.known_runs()
            if known is not None and not set(runs) <= set(known):
                raise ConfigError(f"the ntuples contain runs {sorted(set(runs) - set(known))} that the RAW "
                                  f"entry {meas.raw.id} does not have ({known}); wrong RAW entry?")
        first = next((c["path"] for c in checks if c["n_events"] > 0), inputs.files[0])
    else:
        fmt = meas.counting["format"] or detect_format(inputs.files[0])
        first = inputs.files[0]
    process, l1_process, menu = _check_first_file(meas, fmt, first)

    # ---- jobs ---------------------------------------------------------------------------------------
    n_events = [c["n_events"] for c in checks] if checks else [None] * len(inputs.files)
    groups = split_jobs(n_events, meas.jobs["events_per_job"] if checks else None, meas.jobs["files_per_job"])
    args = count_args(meas, fmt, process, l1_process)
    lumi_mask = meas.lumi_mask.to_json() if meas.lumi_mask else None
    jobs = []
    for job_id, group in enumerate(groups):
        files = [inputs.files[i] for i in group]
        jobs.append({"id": job_id, "files": files,
                     "n_events": sum(n_events[i] for i in group) if checks else None,
                     "output": WorkDir.partial_relpath(job_id), "key": job_key(files, args, lumi_mask)})

    # ---- write the work dir -----------------------------------------------------------------------
    wd.make_dirs()
    _reuse_partials(wd, jobs)
    for path in glob.glob(wd.p("jobs", "job_*.json")):
        os.remove(path)
    shutil.copyfile(meas.config_path, wd.config_copy)
    snapshot_code(wd)
    write_json(wd.files_json, {"specs": meas.ntuples.inputs, "base_dir": meas.ntuples.base_dir,
                               "checked": checks is not None,
                               "duplicates_dropped": [list(d) for d in inputs.duplicates],
                               "files": checks if checks else [{"path": p} for p in inputs.files]})
    for job in jobs:
        write_json(wd.job_json(job["id"]), {"schema": JOB_SCHEMA, "measurement": meas.name, **job,
                                            "count_args": args, "lumi_mask": lumi_mask})
    write_json(wd.jobs_json, {"schema": JOB_SCHEMA, "count_args": args, "jobs": jobs})
    total_events = sum(n_events) if checks else None
    resolved = {
        "schema": WORKDIR_SCHEMA,
        "created": datetime.datetime.now(datetime.timezone.utc).isoformat(timespec="seconds"),
        "steamrates_version": __version__,
        "code_git": git_state(os.path.dirname(os.path.abspath(__file__))),
        "catalog": {"dir": meas.catalog_dir, "git": git_state(meas.catalog_dir) if meas.catalog_dir else None},
        "config_path": meas.config_path,
        "config_sha1": hashlib.sha1(meas.config_text.encode()).hexdigest(),
        "settings_sha1": settings_sha1,
        "settings": settings,
        "warnings": meas.warnings,
        "inputs": {"n_files": len(inputs.files), "n_events": total_events, "n_duplicates_dropped": len(inputs.duplicates),
                   "format": fmt, "process": process, "l1_process": l1_process, "hlt_menu": menu},
        "n_jobs": len(jobs),
    }
    write_json(wd.resolved_json, resolved)       # written last: marks the work dir as prepared
    _print_summary(wd, meas, resolved, jobs)
    return wd, True


def _print_summary(wd, meas, resolved, jobs):
    inp = resolved["inputs"]
    norm = meas.normalization()
    src = lambda in_cat: "catalog" if in_cat else "config"   # noqa: E731
    lines = [f"Prepared {wd}",
             f"  input:         ntuples {meas.ntuples.id} ({src(meas.ntuples_in_catalog)}) "
             f"<- RAW {meas.raw.id} ({src(meas.raw_in_catalog)})",
             f"  files:         {inp['n_files']} ({inp['n_duplicates_dropped']} duplicates dropped), "
             + (f"{inp['n_events']:,} events, " if inp["n_events"] is not None else "")
             + f"format {inp['format']}, process {inp['process']}"
             + (f", L1 process {inp['l1_process']}" if inp["l1_process"] else ""),
             f"  HLT menu:      {inp['hlt_menu']}"]
    if meas.lumi_mask:
        lines.append(f"  lumi mask:     {meas.lumi_mask.n_lumis()} lumi sections in runs {sorted(meas.lumi_mask.ranges)}")
    lines.append("  normalization: " + ", ".join(f"{k} {v}" for k, v in norm.items()))
    lines.append(f"  jobs:          {len(jobs)}"
                 + (f" (~{meas.jobs['events_per_job']:,} events each)" if inp["n_events"] is not None
                    and not meas.jobs["files_per_job"] else f" ({meas.jobs['files_per_job']} files each)"))
    print("\n".join(lines))


def main(argv=None):
    p = argparse.ArgumentParser(prog="python3 -m steamrates prepare", description=__doc__.split("\n\n")[0],
                                formatter_class=argparse.RawDescriptionHelpFormatter, epilog=__doc__.split("\n\n", 1)[1])
    p.add_argument("config", help="measurement config (YAML)")
    p.add_argument("-w", "--workdir", required=True, help="work dir of the measurement (created if needed)")
    p.add_argument("--force", action="store_true", help="update a work dir prepared with other settings/inputs")
    p.add_argument("--catalog", help="sample catalog directory (default: the config's 'catalog' or sample_catalog/)")
    p.add_argument("-j", "--threads", type=int, help="threads for opening the input files (default: jobs.check_threads)")
    p.add_argument("-v", "--verbose", action="store_true")
    args = p.parse_args(argv)
    logging.basicConfig(level=logging.DEBUG if args.verbose else logging.INFO,
                        format="%(asctime)s %(name)s %(levelname)s: %(message)s")
    try:
        prepare(args.config, args.workdir, force=args.force, catalog_dir=args.catalog, threads=args.threads)
    except (ConfigError, InputError) as e:
        raise SystemExit(f"error: {e}") from None
    return 0


if __name__ == "__main__":
    sys.exit(main())
