"""Compare steamrates counts with the legacy workflow's output.path.misc.csv (``nomaps`` mode).

Usage (after ``python3 -m steamrates.summary partial_*.root --pure-reference '^HLT_' --csv new.csv``)::

    python3 validation/compare_legacy_counts.py new.csv <legacy>/Results/Data/output.path.misc.csv \\
        <legacy>/triggerstoexclude.py

Legacy definitions reproduced here:
  * total count: events in which the path fired;
  * pure count: the path fired and no other ``HLT_*`` path fired (all ``HLT_*`` paths count as
    "other", including the excluded ones); paths not starting with ``HLT_`` and paths in
    ``triggerstoexclude.py`` (compared after the legacy ``rstrip("0123456789")``) get 0.
Exit code 0 if everything agrees.
"""

import csv
import sys


def main(new_csv, old_csv, exclude_py):
    new = {r["bit"]: (float(r["count"]), float(r["pure"])) for r in csv.DictReader(open(new_csv))}
    ns = {}
    exec(open(exclude_py).read(), ns)
    excluded = set(ns["triggerstoexcl"])
    old = {}
    for row in csv.reader(open(old_csv)):
        if len(row) < 8 or row[0].startswith(("Path", "Total")):
            continue
        old[row[0].strip()] = (float(row[4]), float(row[6]))

    missing = sorted(p for p in old if p not in new)
    extra = sorted(p for p in new if p not in old and p not in ("HLTriggerFirstPath", "HLTriggerFinalPath"))
    bad_total = [(p, old[p][0], new[p][0]) for p in old if p in new and old[p][0] != new[p][0]]
    bad_pure = []
    for p in old:
        if p not in new:
            continue
        expected = 0.0 if (not p.startswith("HLT_") or p.rstrip("0123456789") in excluded) else new[p][1]
        if old[p][1] != expected:
            bad_pure.append((p, old[p][1], expected))

    print(f"legacy paths: {len(old)}   new bits: {len(new)}")
    print(f"only in legacy: {missing[:10]}   only in new: {extra[:10]}")
    print(f"total-count mismatches: {len(bad_total)} {bad_total[:5]}")
    print(f"pure-count mismatches:  {len(bad_pure)} {bad_pure[:5]}")
    return 0 if not (missing or bad_total or bad_pure) else 1


if __name__ == "__main__":
    sys.exit(main(*sys.argv[1:4]))
