"""run: prepare a work dir and run all its jobs locally (merge follows once implemented).

    python3 -m steamrates run meas.yaml -w WORKDIR [-j 8] [--force] [--catalog DIR]

Equivalent to ``prepare`` followed by ``submit --local``. Running it again only runs the jobs that
are not done yet.
"""

from __future__ import annotations

import argparse
import logging
import sys

from .batch import default_parallel, submit
from .inputs import InputError
from .prepare import prepare
from .schema import ConfigError


def main(argv=None):
    p = argparse.ArgumentParser(prog="python3 -m steamrates run", description=__doc__.split("\n\n")[0])
    p.add_argument("config", help="measurement config (YAML)")
    p.add_argument("-w", "--workdir", required=True)
    p.add_argument("-j", "--parallel", type=int, help=f"local jobs at a time (default: {default_parallel()})")
    p.add_argument("--force", action="store_true", help="update a work dir prepared with other settings/inputs")
    p.add_argument("--catalog", help="sample catalog directory")
    args = p.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s: %(message)s")
    try:
        wd, _ = prepare(args.config, args.workdir, force=args.force, catalog_dir=args.catalog)
    except (ConfigError, InputError) as e:
        raise SystemExit(f"error: {e}") from None
    states = submit(wd, local=True, n_parallel=args.parallel)
    if not all(s == "done" for _, s, _ in states):
        print("Some jobs are not done; fix the problem and repeat the command (only those jobs run again).")
        return 1
    print("All jobs done. (The merge into rates is not implemented yet.)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
