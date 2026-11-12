"""merge: the partial results of a work dir -> normalized rates (JSON for the website, CSV for Excel).

    python3 -m steamrates merge WORKDIR [--allow-missing] [--from-counters] [-o DIR]

Steps:
  1. Check the jobs (steamrates.jobstatus). All must be done; with ``--allow-missing`` the merge uses
     the finished ones and flags the result as incomplete, with the list of missing jobs.
  2. First pass over the partials: metadata, lumi sections and counters only (cheap). Collects the
     bit list, the menu, the input files (a file counted twice stops the merge), the lumi sections
     and the MC sums.
  3. Maps (steamrates.maps): datasets, streams, categories and pure-rate references, from the menu
     or from the files given in ``merge.datasets``/``merge.streams``. Fired bits that are not in the
     menu of the counted process stop the merge.
  4. Second pass, one partial at a time:
       * its combination table is compared with its old-style counters (steamrates.counters), which
         were filled directly in the event loop; any difference stops the merge;
       * the table is added to the rate accumulator (steamrates.rates).
     Memory: one partial's table (processed in blocks) plus the accumulators, independent of the
     number of jobs.
  5. Normalization (steamrates.normalize) and the output files (steamrates.results) in
     ``WORKDIR/results/`` (or ``-o DIR``).

``--from-counters`` (automatic if a partial has no table, i.e. ``counting: {table: false}``) uses the
counters only: path rates, pure rates with the count-time definition, and the count-time totals; no
datasets, streams or overlaps.

The merge settings are those of the prepared work dir (``config.resolved.json``). To change them,
edit the config and run ``prepare --force`` (or ``run``) again: finished jobs are kept, because the
merge settings do not change what a job counts.
"""

from __future__ import annotations

import argparse
import copy
import datetime
import json
import logging
import resource
import sys
import time
from dataclasses import dataclass, field

import numpy as np

from . import __version__
from .config import MERGE as MERGE_SPEC
from .config import OVERLAPS as OVERLAP_DEFAULTS
from .counters import FIELDS as COUNTER_FIELDS
from .counters import counters_from_table
from .jobstatus import compact_ids, job_states
from .lumimask import LumiMask
from .maps import MapsError, build_maps
from .menu import strip_version
from .normalize import NormalizationError
from .normalize import build as build_normalization
from .partial import PartialResult
from .rates import RateAccumulator, default_overlaps, default_totals
from .workdir import WorkDir

log = logging.getLogger("steamrates.merge")

SELECTION_PATH_FRACTION = 0.99      # a pure-rate reference path firing this often probably selects the sample


class MergeError(Exception):
    pass


def merge_settings(settings):
    """The ``merge`` settings of a work dir, with defaults for keys added after it was prepared."""
    m = copy.deepcopy(settings.get("merge") or {})
    for key, (_, default, _) in MERGE_SPEC.items():
        m.setdefault(key, copy.deepcopy(default))
    m["overlaps"] = dict(OVERLAP_DEFAULTS, **(m.get("overlaps") or {}))
    return m


# --------------------------------------------------------------------------------------------------
# Pass 1: metadata, lumi sections, counters
# --------------------------------------------------------------------------------------------------

@dataclass
class Collected:
    bits: list = field(default_factory=list)              # merged bit names (after renaming)
    menu: dict | None = None                              # menu of the first partial that has one
    menu_names: list = field(default_factory=list)        # distinct menu names seen
    inputs: list = field(default_factory=list)            # per-file records of all partials
    lumi_sets: dict = field(default_factory=dict)         # sample -> {"in_file": set, "with_events": set}
    events: dict = field(default_factory=lambda: {"read": 0, "selected": 0, "sumw": 0.0, "sumw2": 0.0,
                                                  "duplicate_events": 0})
    weight_branches: set = field(default_factory=set)
    formats: set = field(default_factory=set)
    bit_processes: list = field(default_factory=list)
    all_have_table: bool = True
    counters: dict | None = field(default_factory=dict)   # bit -> 6 sums (None if a partial has none)
    counter_definitions: set = field(default_factory=set)  # JSON of each distinct definition
    counter_totals: dict = field(default_factory=dict)    # label -> 3 sums
    jobs: list = field(default_factory=list)


def _definition_key(d):
    return json.dumps({"pure_reference": d["pure_reference"], "exclude": d["exclude"],
                       "strip_versions": d["strip_versions"],
                       "totals": {k: v["regex"] for k, v in d["totals"].items()}}, sort_keys=True)


def collect(partial_paths, rename):
    """First pass over the partials (no tables)."""
    c = Collected()
    index = {}
    for path in partial_paths:
        p = PartialResult.read(path, table=False)
        meta = p.metadata
        names = [rename(b) for b in meta["bits"]]
        if len(set(names)) != len(names):
            raise MergeError(f"{path}: bit names become ambiguous after removing the versions")
        for n in names:
            if n not in index:
                index[n] = len(c.bits)
                c.bits.append(n)
        if meta.get("menu"):
            if c.menu is None:
                c.menu = meta["menu"]
            if meta["menu"].get("name") not in c.menu_names:
                c.menu_names.append(meta["menu"].get("name"))
        c.inputs.extend(meta["inputs"])
        s = c.lumi_sets.setdefault(meta.get("sample", "default"), {"in_file": set(), "with_events": set()})
        lumis = p.lumis
        for r, ls, n, inf in zip(lumis["run"].tolist(), lumis["luminosityBlock"].tolist(),
                                 lumis["n_events"].tolist(), lumis["in_file"].tolist()):
            if n > 0:
                s["with_events"].add((r, ls))
            if inf:
                s["in_file"].add((r, ls))
        t = meta["totals"]
        c.events["read"] += t["n_events_read"]
        c.events["selected"] += t["n_events_selected"]
        c.events["sumw"] += t["sumw"]
        c.events["sumw2"] += t["sumw2"]
        c.events["duplicate_events"] += meta.get("duplicate_events", 0)
        c.weight_branches.add(meta.get("weight_branch"))
        c.formats.add(meta.get("format"))
        if meta.get("bit_processes") and meta["bit_processes"] not in c.bit_processes:
            c.bit_processes.append(meta["bit_processes"])
        c.all_have_table &= bool(meta.get("has_table", True))
        if p.counters is None or c.counters is None:
            c.counters = None
        else:
            c.counter_definitions.add(_definition_key(meta["counters"]))
            for f_i, f in enumerate(COUNTER_FIELDS):
                vals = p.counters[f]
                for n, v in zip(names, vals.tolist()):
                    c.counters.setdefault(n, np.zeros(len(COUNTER_FIELDS)))[f_i] += v
            for label, tot in meta["counters"]["totals"].items():
                c.counter_totals.setdefault(label, np.zeros(3))
                c.counter_totals[label] += [tot["count"], tot["sumw"], tot["sumw2"]]
        c.jobs.append((meta.get("job") or {}).get("id"))
    return c


def duplicate_inputs(records):
    seen, dup = set(), set()
    for r in records:
        (dup if r["path"] in seen else seen).add(r["path"])
    return sorted(dup)


# --------------------------------------------------------------------------------------------------
# Pass 2: tables -> cross-check with the counters, rate accumulator
# --------------------------------------------------------------------------------------------------

def crosscheck(partial, path):
    """Compare a partial's counters with the same numbers computed from its table.

    Returns a list of differences (empty: identical). Counts must be identical; sums of weights
    may differ by rounding only (they are added in a different order).
    """
    if partial.counters is None or partial.table is None:
        return []
    arrays, totals = counters_from_table(partial.table, partial.metadata["counters"])
    problems = []
    for f in COUNTER_FIELDS:
        a, b = arrays[f], partial.counters[f].astype(np.float64)
        bad = ~np.isclose(a, b, rtol=1e-9, atol=1e-9 * max(1.0, float(np.abs(b).max(initial=0))))
        if f.endswith("count"):
            bad = a != b
        for i in np.flatnonzero(bad)[:3]:
            problems.append(f"{path}: {partial.table.bit_names[i]} {f}: table {a[i]:g}, counters {b[i]:g}")
    for label, t in partial.metadata["counters"]["totals"].items():
        for f in ("count", "sumw", "sumw2"):
            if not np.isclose(totals[label][f], t[f], rtol=1e-9, atol=1e-9):
                problems.append(f"{path}: total {label} {f}: table {totals[label][f]:g}, counters {t[f]:g}")
    return problems


# --------------------------------------------------------------------------------------------------
# The merge
# --------------------------------------------------------------------------------------------------

@dataclass
class MergeResult:
    """Everything the output files are built from (steamrates.results)."""
    workdir: str
    resolved: dict
    settings: dict                       # merge settings used
    collected: Collected
    maps: object
    normalization: object
    accumulator: RateAccumulator | None  # None with --from-counters
    from_counters: bool
    complete: bool
    missing_jobs: list
    crosscheck: dict
    warnings: list
    timing: dict


def merge(workdir, allow_missing=False, from_counters=False, block=None):
    """Merge the partials of a work dir. Returns a :class:`MergeResult` (nothing written yet)."""
    t0 = time.time()
    wd = WorkDir(workdir)
    if not wd.is_prepared():
        raise MergeError(f"{wd} is not a prepared work dir")
    resolved = wd.resolved()
    settings = resolved["settings"]
    mset = merge_settings(settings)
    rename = strip_version if mset["strip_versions"] else (lambda n: n)
    warnings = list(resolved.get("warnings") or [])

    # ---- 1. jobs ------------------------------------------------------------------------------------
    states = job_states(wd)
    done = [j for j, s, _ in states if s == "done"]
    missing = [j["id"] for j, s, _ in states if s != "done"]
    if missing and not allow_missing:
        raise MergeError(f"{len(missing)} of {len(states)} jobs are not done ({compact_ids(missing)}); "
                         "see 'status'. Use --allow-missing to merge the finished ones (flagged incomplete).")
    if not done:
        raise MergeError("no job is done yet")
    if missing:
        warnings.append(f"INCOMPLETE: {len(missing)} of {len(states)} jobs are missing ({compact_ids(missing)}); "
                        "data: the rates of lumi sections spread over several jobs come out too low")
    paths = [wd.partial(j["id"]) for j in done]

    # ---- 2. first pass -----------------------------------------------------------------------------
    log.info("reading the metadata of %d partial results", len(paths))
    c = collect(paths, rename)
    dup = duplicate_inputs(c.inputs)
    if dup:
        raise MergeError(f"{len(dup)} input file(s) were counted more than once, e.g. {dup[0]}")
    if len(c.menu_names) > 1:
        warnings.append(f"the partials have different menus {c.menu_names}; datasets and streams are taken "
                        f"from {c.menu_names[0]}")
    if len(c.weight_branches) > 1:
        raise MergeError(f"the partials were counted with different weight branches: {sorted(map(str, c.weight_branches))}")
    if c.events["duplicate_events"]:
        warnings.append(f"{c.events['duplicate_events']} events appeared more than once in a job "
                        "(counted with allow_duplicate_events)")
    if not c.all_have_table and not from_counters:
        warnings.append("some partials have no combination table (count --no-table): merged from the counters")
        from_counters = True
    if from_counters:
        if c.counters is None:
            raise MergeError("--from-counters: some partials have no counters")
        if len(c.counter_definitions) > 1:
            raise MergeError("--from-counters: the partials were counted with different counter definitions "
                             "(pure reference, exclude, totals); recount them or merge from the tables")

    # ---- 3. maps ------------------------------------------------------------------------------------
    try:
        maps = build_maps(c.bits, c.menu, mset)
    except MapsError as e:
        raise MergeError(str(e)) from None
    warnings.extend(maps.warnings)
    if c.menu is None:
        warnings.append("the ntuples have no HLT menu: the bits cannot be checked against it")

    # ---- 4. second pass ----------------------------------------------------------------------------
    acc = None
    check = {"partials_checked": 0, "differences": []}
    if not from_counters:
        acc = RateAccumulator(maps, default_totals(maps, mset["totals"]), default_overlaps(maps, mset["overlaps"]),
                              **({"block": block} if block else {}))
        for i, path in enumerate(paths, 1):
            p = PartialResult.read(path)
            diffs = crosscheck(p, path)
            if p.counters is not None:
                check["partials_checked"] += 1
            check["differences"].extend(diffs)
            acc.add_table(p.table.rename(rename) if mset["strip_versions"] else p.table)
            del p
            if i % 50 == 0 or i == len(paths):
                log.info("added %d/%d tables (%d rows)", i, len(paths), acc.n_rows)
        if check["differences"]:
            raise MergeError("the combination tables do not reproduce the counters of the event loop:\n  "
                             + "\n  ".join(check["differences"][:20]))
        fired = acc.sums["bits"]["fired"][:, 0]
    else:
        fired = np.array([c.counters[b][0] for b in maps.bits])

    not_in_menu = maps.bits_not_in_menu()
    fired_not_in_menu = [maps.bits[i] for i in not_in_menu if fired[i] > 0]
    if fired_not_in_menu:
        raise MergeError(f"{len(fired_not_in_menu)} fired bit(s) are not paths of the menu "
                         f"{c.menu.get('name') or c.menu.get('process')}: {fired_not_in_menu[:10]}")

    # ---- 5. normalization --------------------------------------------------------------------------
    norm_cfg = settings["normalization"]
    try:
        if norm_cfg["mode"] == "data":
            if len(c.lumi_sets) != 1:
                raise MergeError(f"the partials belong to several samples {sorted(c.lumi_sets)}")
            mask = settings.get("lumi_mask")
            norm = build_normalization(norm_cfg, lumi_sets=next(iter(c.lumi_sets.values())),
                                       lumi_mask_n=LumiMask.from_json(mask).n_lumis() if mask else None)
        else:
            weighted = next(iter(c.weight_branches)) is not None
            norm = build_normalization(norm_cfg, input_records=c.inputs,
                                       sumw_processed=c.events["sumw"] if weighted else c.events["selected"],
                                       weighted=weighted)
    except NormalizationError as e:
        raise MergeError(f"normalization: {e}") from None
    warnings.extend(norm.warnings)
    if missing and norm_cfg["mode"] != "data":
        warnings.append("MC with missing jobs: the Runs-tree sums of the missing files are not counted either")

    # ---- sanity checks on the content --------------------------------------------------------------
    n_input = c.events["selected"]
    if acc is not None and n_input >= 100:
        for i in range(maps.n_bits):
            if maps.bit_reference(i) in ("standard_parking", "scouting") and fired[i] >= SELECTION_PATH_FRACTION * n_input:
                warnings.append(f"{maps.bits[i]} fired in {fired[i] / n_input:.1%} of the events: if it selects "
                                "the sample, add it to merge.exclude, otherwise the pure rates of its "
                                "reference group are ~0 and the totals ~100%")
    sel_paths = (settings["input"]["raw"].get("selection_paths") or [])
    for s in sel_paths:
        name = rename(s)
        if name in maps.bits and not maps.excluded[maps.bits.index(name)]:
            warnings.append(f"the sample's selection path {name} is not in merge.exclude: it is counted in the "
                            "totals and the pure-rate references like any other path")

    usage = resource.getrusage(resource.RUSAGE_SELF)
    timing = {"runtime_s": round(time.time() - t0, 1), "cpu_s": round(usage.ru_utime + usage.ru_stime, 1),
              "max_rss_mb": round(usage.ru_maxrss / 1024),
              "rows": acc.n_rows if acc else None, "partials": len(paths)}
    return MergeResult(workdir=wd.path, resolved=resolved, settings=mset, collected=c, maps=maps,
                       normalization=norm, accumulator=acc, from_counters=from_counters,
                       complete=not missing, missing_jobs=missing, crosscheck=check, warnings=warnings, timing=timing)


def main(argv=None):
    from .results import print_summary, write_results
    p = argparse.ArgumentParser(prog="python3 -m steamrates merge", description=__doc__.split("\n\n")[0],
                                formatter_class=argparse.RawDescriptionHelpFormatter,
                                epilog=__doc__.split("\n\n", 1)[1])
    p.add_argument("workdir")
    p.add_argument("--allow-missing", action="store_true", help="merge the finished jobs only (flagged incomplete)")
    p.add_argument("--from-counters", action="store_true", help="use the old-style counters instead of the tables")
    p.add_argument("-o", "--output", help="output directory (default: WORKDIR/results)")
    p.add_argument("-v", "--verbose", action="store_true")
    args = p.parse_args(argv)
    logging.basicConfig(level=logging.DEBUG if args.verbose else logging.INFO,
                        format="%(asctime)s %(name)s %(levelname)s: %(message)s")
    try:
        result = merge(args.workdir, allow_missing=args.allow_missing, from_counters=args.from_counters)
    except MergeError as e:
        raise SystemExit(f"error: {e}") from None
    out_dir = args.output or WorkDir(args.workdir).p("results")
    summary = write_results(result, out_dir)
    print_summary(result, summary, out_dir)
    return 0


def created_now():
    return datetime.datetime.now(datetime.timezone.utc).isoformat(timespec="seconds")


__all__ = ["merge", "MergeError", "MergeResult", "merge_settings", "created_now", "__version__"]

if __name__ == "__main__":
    sys.exit(main())
