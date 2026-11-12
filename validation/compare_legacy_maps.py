"""Compare steamrates dataset and stream counts with the legacy ``somemaps``/``allmaps`` outputs.

The legacy counting (``Rates/triggerCountsFromTriggerResults.py -m somemaps``) runs one job per
input file and writes per-job CSV files to ``Results/Data/Raw/<Category>/``. Jobs whose outputs got
lost are missing there, so the comparison uses exactly the jobs that have all outputs:

1. List the input files of the complete legacy jobs::

       python3 validation/compare_legacy_maps.py files <legacy Rates dir> > files.txt

2. Count them with steamrates (a measurement whose input is ``files.txt``, e.g. via an own-ntuples
   config) and merge.

3. Compare::

       python3 validation/compare_legacy_maps.py compare <legacy Rates dir> <steamrates work dir>

The legacy dataset/stream maps come from its ``Menu_HLT.py``, which is usually older than the menu
in the ntuples. A dataset (stream) is therefore compared only if its legacy path list equals the
menu's (versions stripped, excluded paths ignored); the others are listed. Counts must be identical.

The legacy count of a dataset is the OR of its paths, also for split datasets (EGamma0 ... EGamma5
each get the count of all of EGamma), and excluded paths stay in it: it is compared with the count
of the combined dataset (stream) in ``datasets.json`` (``streams.json``), from a measurement with
``merge: {default_exclude: false}`` and no ``exclude``.
"""

from __future__ import annotations

import csv
import glob
import json
import os
import re
import sys
from collections import defaultdict

CATEGORIES = ("Global", "Stream", "DatasetPhysics", "DatasetMisc")


def legacy_jobs(rates_dir):
    """{job index: input file} of the jobs that have a non-empty output in every category.

    (Some legacy outputs exist but are empty: e.g. job 2960 of the 403663 baseline has an empty
    dataset file while its other outputs are complete.)
    """
    raw = os.path.join(rates_dir, "Results", "Data", "Raw")
    have = None
    for cat in CATEGORIES:
        d = os.path.join(raw, cat)
        ids = {int(re.search(r"\.(\d+)\.csv$", f).group(1)) for f in os.listdir(d)
               if os.path.getsize(os.path.join(d, f)) > 0}
        have = ids if have is None else have & ids
    files = {}
    for sub in glob.glob(os.path.join(rates_dir, "Jobs", "sub_raw", "sub_*.sh")):
        text = open(sub).read()
        m = re.search(r"triggerCountsFromTriggerResults\.py -i (\S+).* -s (\d+)", text)
        if m and int(m.group(2)) in have:
            files[int(m.group(2))] = os.path.normpath(m.group(1))
    return files


def legacy_sums(rates_dir, jobs, pattern, key_col=0):
    """Sum the per-job CSV counts (column 'Counts') over the given jobs."""
    raw = os.path.join(rates_dir, "Results", "Data", "Raw")
    out = defaultdict(int)
    for i in jobs:
        for path in glob.glob(os.path.join(raw, pattern.format(i=i))):
            for row in csv.reader(open(path)):
                if not row or row[0].strip() in ("Dataset", "Streams", "Stream"):
                    continue
                out[row[key_col].strip()] += int(row[1])
    return out


def legacy_maps(rates_dir):
    sys.path.insert(0, rates_dir)
    import Menu_HLT  # noqa: E402  (the legacy map module of that directory)
    strip = lambda p: re.sub(r"_v$", "", p)   # noqa: E731  legacy names end in "_v" (rstrip of digits)
    ds, st = defaultdict(set), defaultdict(set)
    for path, dss in Menu_HLT.datasetMap.items():
        for d in dss:
            ds[d].add(strip(path))
    for path, sts in Menu_HLT.streamMap.items():
        for s in sts:
            st[s].add(strip(path))
    return ds, st


def compare(rates_dir, workdir):
    jobs = legacy_jobs(rates_dir)
    res = os.path.join(workdir, "results")
    summary = json.load(open(os.path.join(res, "summary.json")))
    n_new = summary["events"]["selected"]
    load = lambda name: {r["name"]: r for r in json.load(open(os.path.join(res, name)))["records"]}   # noqa: E731
    combined_ds, combined_st = load("datasets.json"), load("streams.json")
    # each split dataset/stream of the menu -> its combined record (same paths, same legacy count)
    datasets = {d: combined_ds[r["combined"]] for d, r in load("datasets_split.json").items()}
    streams = {s: combined_st[r["combined"]] for s, r in load("streams_split.json").items()}
    if summary["exclude_patterns"]:
        print(f"WARNING: the measurement excludes paths ({len(summary['exclude_patterns'])} patterns): legacy keeps "
              "them in the dataset/stream counts; use merge: {default_exclude: false}")
    old_ds = legacy_sums(rates_dir, jobs, "DatasetPhysics/output.dataset.physics.{i}.csv")
    for k, v in legacy_sums(rates_dir, jobs, "DatasetMisc/output.dataset.misc.{i}.csv").items():
        old_ds.setdefault(k, v)            # a dataset is in one of the two files
    old_st = legacy_sums(rates_dir, jobs, "Stream/output.stream.{i}.csv")
    lmap_ds, lmap_st = legacy_maps(rates_dir)
    events_old = old_ds.pop("unknown", None)
    print(f"legacy jobs with all outputs: {len(jobs)}; events: legacy 'unknown' dataset (= all events) "
          f"{events_old}, steamrates {n_new}")
    status = 0
    for what, old, new, lmap, members in (
            ("dataset", old_ds, datasets, lmap_ds, lambda r: set(r["paths"])),
            ("stream", old_st, streams, lmap_st, lambda r: {p for d in r["datasets"] for p in combined_ds[d]["paths"]})):
        same, differ, skipped = [], [], []
        for name, count in sorted(old.items()):
            if name not in new:
                skipped.append(f"{name} (not in the menu)")
                continue
            if lmap.get(name, set()) != members(new[name]):
                skipped.append(f"{name} (path lists differ: {len(lmap.get(name, ()))} legacy vs "
                               f"{len(members(new[name]))})")
                continue
            (same if count == new[name]["count"] else differ).append((name, count, new[name]["count"]))
        print(f"\n{what}s: {len(same)} identical, {len(differ)} DIFFERENT, {len(skipped)} not comparable")
        for name, a, b in differ:
            print(f"  DIFFERENT {name}: legacy {a}, steamrates {b}")
        for s in skipped:
            print(f"  not compared: {s}")
        status |= bool(differ)
    return status


def main(argv):
    if len(argv) >= 2 and argv[0] == "files":
        for _, f in sorted(legacy_jobs(argv[1]).items()):
            print(f)
        return 0
    if len(argv) >= 3 and argv[0] == "compare":
        return compare(argv[1], argv[2])
    print(__doc__)
    return 2


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
