"""Command-line entry point: ``python3 -m steamrates <command> [options]``.

Commands:
    prepare   measurement config -> work dir with job files
    catalog   browse the sample catalog (list, show ID)
    count     count trigger bits of ntuple files -> one partial result (what a job runs)
    summary   inspect / add up partial results (raw counts)
"""

from __future__ import annotations

import importlib
import sys

COMMANDS = {
    "prepare": "steamrates.prepare",
    "catalog": "steamrates.catalog",
    "count": "steamrates.count",
    "summary": "steamrates.summary",
}


def main(argv=None):
    argv = sys.argv[1:] if argv is None else argv
    if not argv or argv[0] in ("-h", "--help"):
        print(__doc__)
        return 0
    command, rest = argv[0], argv[1:]
    if command not in COMMANDS:
        print(f"unknown command {command!r}\n{__doc__}", file=sys.stderr)
        return 2
    return importlib.import_module(COMMANDS[command]).main(rest)


if __name__ == "__main__":
    sys.exit(main())
