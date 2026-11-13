"""run: prepare a work dir, run all its jobs and merge the results.

    python3 -m steamrates run meas.yaml -w WORKDIR [-j 8 | --condor] [--force] [--catalog DIR]

Equivalent to ``prepare``, ``submit --local`` (or ``submit --condor --wait``) and ``merge``.
Running it again only runs the jobs that are not done yet, and merges again (e.g. after changing
merge settings with ``--force``). With ``--condor`` it waits for the jobs (Ctrl-C stops waiting;
the jobs keep running, and running the command again continues).
"""

from __future__ import annotations

import argparse
import logging
import sys

from .batch import default_parallel, submit, wait_condor
from .inputs import InputError
from .jobstatus import print_status
from .merge import MergeError, merge
from .prepare import prepare
from .results import print_summary, write_results
from .schema import ConfigError


def main(argv=None):
    p = argparse.ArgumentParser(prog="python3 -m steamrates run", description=__doc__.split("\n\n")[0])
    p.add_argument("config", help="measurement config (YAML)")
    p.add_argument("-w", "--workdir", required=True)
    p.add_argument("-j", "--parallel", type=int, help=f"local jobs at a time (default: {default_parallel()})")
    p.add_argument("--condor", action="store_true", help="run the jobs on HTCondor (lxplus) and wait for them")
    p.add_argument("--poll", type=int, default=60, help="--condor: seconds between queue checks (default: 60)")
    p.add_argument("--force", action="store_true", help="update a work dir prepared with other settings/inputs")
    p.add_argument("--catalog", help="sample catalog directory")
    args = p.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s: %(message)s")
    try:
        wd, _ = prepare(args.config, args.workdir, force=args.force, catalog_dir=args.catalog)
    except (ConfigError, InputError) as e:
        raise SystemExit(f"error: {e}") from None
    states = submit(wd, local=not args.condor, n_parallel=args.parallel)
    if args.condor:
        states = wait_condor(wd, args.poll)
        print_status(wd, states, tail=3)
    if not all(s == "done" for _, s, _ in states):
        print("Some jobs are not done; fix the problem and repeat the command (only those jobs run again).")
        return 1
    try:
        result = merge(wd.path)
    except MergeError as e:
        raise SystemExit(f"error: merge: {e}") from None
    out_dir = wd.p("results")
    print_summary(result, write_results(result, out_dir), out_dir)
    return 0


if __name__ == "__main__":
    sys.exit(main())
