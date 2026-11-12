"""Output files of the merge step (written to ``WORKDIR/results/``).

JSON (for the website; every file has ``schema`` and ``measurement``)::

    summary.json                 the measurement, input, normalization, totals, checks, warnings, settings
    paths_hlt.json               one record per HLT-side bit (HLT_, DST_, AlCa_, ..., Dataset_ paths)
    paths_l1.json                one record per L1 bit
    datasets.json, streams.json  one record per combined dataset / stream (split ones together,
                                 e.g. JetMET = JetMET0 + JetMET1): rates, pure rates, written rates
    datasets_split.json,         one record per dataset / stream of the menu (JetMET0, JetMET1, ...):
    streams_split.json           what each one writes (the numbers to control the bandwidth)
    excluded_paths.json          the bits left out of all combined quantities, with the matching pattern
    paths_without_dataset.json   paths in no dataset (to review; exclude them with merge.exclude if wanted)
    overlaps_<name>.json         overlap matrices (sparse): datasets, streams, paths_datasets, l1_datasets,
                                 l1_streams, paths (HLT x HLT); datasets and streams are the combined ones

CSV (for spreadsheets): ``paths_hlt.csv``, ``paths_l1.csv``, ``datasets.csv``, ``streams.csv``,
``datasets_split.csv``, ``streams_split.csv``.

Every rate comes with ``count`` (number of events), ``rate_hz`` and ``rate_err_hz`` (statistical, from
the sum of squared weights). Records are sorted by rate. Pure rates of items without a reference
group (calibration/monitoring, no dataset, excluded) are 0 with ``pure_reference: "none"``. Scouting
paths, datasets and streams also get ``shared_with_standard_parking_*``: the rate of events that a
standard or parking path also selected, and its fraction of the item's rate. Numbers are rounded to
6 significant digits.

Combined and split datasets (steamrates.maps)
---------------------------------------------
A combined dataset fires when one of its paths fires (``rate_hz``). Its ``written_rate_hz`` is the
sum of its split datasets: the same for round-robin splits, lower for datasets with a dataset
prescale (OnlineMonitor). The rate of a split dataset (``method``):

    dataset_path         its Dataset_ path in the ntuples (exact: round robin and dataset prescale)
    paths                the OR of its paths: exact if the dataset has no prescale, neither on its
                         Dataset_ path nor on single paths in its trigger conditions (HLTMonitor,
                         ExpressPhysics and EventDisplay prescale single paths: overestimated)
    paths_div_prescale   no Dataset_ bit: OR of its paths / the prescale of its Dataset_ path in the
                         menu's default column (estimate; e.g. Run-3 NanoAOD)
    paths_div_copies     no prescale table either (MC menus): OR of its paths / number of copies

A split stream (``method``): ``datasets`` = event by event OR of its datasets' decisions (exact when
they are exact); ``sum_of_datasets`` = the sum of its datasets' estimates (all copies of one combined
dataset, which never share an event); ``upper_bound`` = OR of their paths, dataset prescales not
applied (with a warning).

Overlap files list the non-zero entries ``[row, col, count, rate_hz, rate_err_hz]`` (row/col index
into ``rows``/``cols``); for symmetric matrices only ``row <= col``. The diagonal is the rate of the
item itself.
"""

from __future__ import annotations

import csv
import json
import math
import os

import numpy as np
import scipy.sparse as sp

from .config import DEFAULT_EXCLUDE
from .maps import HLT_REFERENCES, is_l1_seed, l1_flavour, menu_prescales
from .merge import created_now
from . import __version__

SCHEMA = "steamrates.results/1"
OVERLAP_SCHEMA = "steamrates.overlaps/1"
KNOWN_FILES = ("summary.json", "paths_hlt.json", "paths_l1.json", "datasets.json", "streams.json",
               "datasets_split.json", "streams_split.json", "excluded_paths.json",
               "paths_without_dataset.json", "paths_hlt.csv", "paths_l1.csv", "datasets.csv", "streams.csv",
               "datasets_split.csv", "streams_split.csv",
               "overlaps_datasets.json", "overlaps_streams.json", "overlaps_paths_datasets.json",
               "overlaps_l1_datasets.json", "overlaps_l1_streams.json", "overlaps_paths.json")
DATASET_METHODS = {
    "dataset_path": "its Dataset_ path in the ntuples (exact: round robin and dataset prescale)",
    "paths": "the OR of its paths (exact unless the dataset prescales single paths in its trigger conditions, "
             "e.g. HLTMonitor, ExpressPhysics, EventDisplay: then too high)",
    "paths_div_prescale": "estimate: OR of its paths / prescale of its Dataset_ path in the menu (no Dataset_ bit)",
    "paths_div_copies": "estimate: OR of its paths / number of copies (no Dataset_ bit, no prescale table)"}
STREAM_METHODS = {
    "datasets": "event by event OR of its datasets' decisions",
    "sum_of_datasets": "sum of its datasets' estimates (copies of one combined dataset never share an event)",
    "upper_bound": "OR of the paths of its datasets: dataset prescales not applied"}
EXACT_DATASET_METHODS = ("dataset_path", "paths")
REFERENCE_LABELS = {"standard_parking": "standard+parking paths", "scouting": "scouting paths",
                    "l1": "L1 seeds", "l1_final": "L1 seeds (final)", "l1_beforePrescale": "L1 seeds (beforePrescale)",
                    "l1_beforeBxMaskAndPrescale": "L1 seeds (beforeBxMaskAndPrescale)"}


def sig(x, digits=6):
    """Round to ``digits`` significant digits (keeps the JSON small and readable)."""
    x = float(x)
    if x == 0 or not math.isfinite(x):
        return 0.0 if x == 0 else x
    return float(f"{x:.{digits}g}")


def rate_fields(s3, scale, prefix=""):
    """``count``, ``rate_hz``, ``rate_err_hz`` from (count, Σw, Σw²)."""
    return {f"{prefix}count": int(round(s3[0])), f"{prefix}rate_hz": sig(s3[1] * scale),
            f"{prefix}rate_err_hz": sig(math.sqrt(max(s3[2], 0.0)) * scale)}


def shared_fields(shared, fired, scale):
    rate = fired[1] * scale
    return {"shared_with_standard_parking_rate_hz": sig(shared[1] * scale),
            "shared_with_standard_parking_rate_err_hz": sig(math.sqrt(max(shared[2], 0.0)) * scale),
            "shared_with_standard_parking_fraction": sig(shared[1] * scale / rate) if rate else 0.0}


def _by_rate(records):
    return sorted(records, key=lambda r: (-r["rate_hz"], r["name"]))


# --------------------------------------------------------------------------------------------------
# Building the documents
# --------------------------------------------------------------------------------------------------

class _Sums:
    """Per-item (count, Σw, Σw²) for fired / pure / shared, from the accumulator or the counters."""

    def __init__(self, result):
        self.r = result
        acc, maps = result.accumulator, result.maps
        if acc is not None:
            self.bits = acc.sums["bits"]
            self.datasets = acc.sums["datasets"]
            self.streams = acc.sums["streams"]
            self.datasets_split = acc.sums["datasets_split"]["fired"]
            self.streams_split = acc.sums["streams_split"]["fired"]
        else:
            c = result.collected.counters
            arr = np.array([c[b] for b in maps.bits]) if maps.bits else np.zeros((0, 6))
            self.bits = {"fired": arr[:, :3], "pure": arr[:, 3:], "shared_sp": np.zeros((len(maps.bits), 3))}
            self.datasets = self.streams = None


def build_documents(result):
    """Returns ``(documents, csv_tables)``: ``{file name: JSON object}``, ``{file name: (header, rows)}``."""
    maps, norm, c = result.maps, result.normalization, result.collected
    scale = norm.scale
    sums = _Sums(result)
    rename = (lambda n: n)
    if result.settings["strip_versions"]:
        from .menu import strip_version
        rename = strip_version
    menu_ps, ps_column = menu_prescales(result.collected.menu, rename)
    name = result.resolved["settings"]["name"]
    head = lambda kind: {"schema": SCHEMA, "kind": kind, "measurement": name, "unit": "Hz"}   # noqa: E731
    from_counters = result.from_counters

    def pure_ref(group):
        if from_counters:
            return "counters"
        return group or "none"

    # ---- HLT-side bits ---------------------------------------------------------------------------
    paths = []
    for i, b in enumerate(maps.bits):
        if maps.kind[i] == "l1":
            continue
        ds = maps.path_combined_datasets(b)
        rec = {"name": b, "kind": maps.kind[i], "category": maps.main_category(i),
               "categories": maps.path_categories[i], "excluded": bool(maps.excluded[i]),
               "datasets": ds, "streams": sorted({s for d in ds for s in maps.combined_dataset_streams(d)}),
               **rate_fields(sums.bits["fired"][i], scale),
               "pure_reference": pure_ref(maps.bit_reference(i)), **rate_fields(sums.bits["pure"][i], scale, "pure_")}
        if ps_column:
            rec["menu_prescale"] = menu_ps.get(b)
        if not from_counters and maps.bit_reference(i) == "scouting":
            rec.update(shared_fields(sums.bits["shared_sp"][i], sums.bits["fired"][i], scale))
        paths.append(rec)
    paths = _by_rate(paths)

    l1 = []
    for i, b in enumerate(maps.bits):
        if maps.kind[i] != "l1":
            continue
        l1.append({"name": b, "seed": is_l1_seed(b), "flavour": l1_flavour(b), "excluded": bool(maps.excluded[i]),
                   **rate_fields(sums.bits["fired"][i], scale),
                   "pure_reference": pure_ref(maps.bit_reference(i)), **rate_fields(sums.bits["pure"][i], scale, "pure_")})
    l1 = _by_rate(l1)

    # ---- datasets and streams (combined and split) ---------------------------------------------
    datasets, streams, datasets_split, streams_split, split_check = [], [], [], [], None
    if sums.datasets is not None and maps.datasets:
        datasets, streams, datasets_split, streams_split, split_check = _dataset_records(result, sums, scale, pure_ref)

    # ---- totals and checks -----------------------------------------------------------------------
    totals, checks = {}, {}
    n_in = c.events["selected"]
    if result.accumulator is not None:
        acc = result.accumulator
        for t in acc.totals:
            s3 = acc.total_sums[t.label]
            totals[t.label] = {"description": t.description, **rate_fields(s3, scale),
                               "fraction_of_input": sig(s3[1] / acc.total_sums["input"][1]) if acc.total_sums["input"][1] else 0.0}
        if "status_oncpu_or_ongpu" in totals:
            ok = totals["status_oncpu_or_ongpu"]["count"] == totals["input"]["count"]
            checks["status_paths"] = {"result": "ok" if ok else "MISMATCH",
                                      "status_count": totals["status_oncpu_or_ongpu"]["count"],
                                      "input_count": totals["input"]["count"]}
            if not ok:
                result.warnings.append(f"Status_OnCPU or Status_OnGPU fired in {totals['status_oncpu_or_ongpu']['count']} "
                                       f"of {totals['input']['count']} events (expected: all)")
    else:
        totals["input"] = {"description": "all events (HLT input)",
                           **rate_fields((n_in, c.events["sumw"] if c.weight_branches != {None} else n_in,
                                          c.events["sumw2"] if c.weight_branches != {None} else n_in), scale)}
        for label, t3 in c.counter_totals.items():
            totals[f"regex_{label}"] = {"description": "count-time counters total", **rate_fields(t3, scale)}
    _input_from_generated(totals, norm, c)
    checks["counters_vs_table"] = ({"result": "not done (merged from the counters)"} if from_counters else
                                   {"result": "ok" if result.crosscheck["partials_checked"] else "no counters in the partials",
                                    "partials_checked": result.crosscheck["partials_checked"]})
    if split_check is not None:
        checks["split_datasets"] = split_check
        if split_check["result"] == "MISMATCH":
            result.warnings.append("split datasets/streams whose Dataset_ bits do not add up to the combined one "
                                   f"(copies share events or miss some): {split_check['mismatches'][:5]}")
    unfired_not_in_menu = [maps.bits[i] for i in maps.bits_not_in_menu()]
    checks["bits_not_in_menu"] = {"menu": (c.menu or {}).get("name"), "process": (c.menu or {}).get("process"),
                                  "never_fired": unfired_not_in_menu}

    excluded = _by_rate([{"name": b, "kind": maps.kind[i], "category": maps.main_category(i),
                          "patterns": maps.excluded_by[b],
                          "default_list": any(x in DEFAULT_EXCLUDE for x in maps.excluded_by[b]),
                          "datasets": maps.path_combined_datasets(b), **rate_fields(sums.bits["fired"][i], scale)}
                         for i, b in enumerate(maps.bits) if maps.excluded[i]])

    without = []
    if maps.datasets:
        for i in maps.paths_without_dataset():
            without.append({"name": maps.bits[i], "excluded": bool(maps.excluded[i]),
                            **rate_fields(sums.bits["fired"][i], scale)})
        without = _by_rate(without)

    # ---- summary ---------------------------------------------------------------------------------
    res = result.resolved
    st = res["settings"]
    count_by = lambda items, key: {cat: sum(1 for r in items if r[key] == cat)   # noqa: E731
                                   for cat in dict.fromkeys(r[key] for r in items)}
    summary = {
        **head("summary"),
        "measurement_info": {"name": name, "workdir": result.workdir, "merged": created_now(),
                             "prepared": res.get("created"), "steamrates_version": __version__,
                             "code_git": res.get("code_git"), "config_path": res.get("config_path"),
                             "config_sha1": res.get("config_sha1"), "settings_sha1": res.get("settings_sha1")},
        "input": {"ntuples": st["input"]["ntuples"], "raw": st["input"]["raw"],
                  "ntuples_in_catalog": st["input"].get("ntuples_in_catalog"),
                  "raw_in_catalog": st["input"].get("raw_in_catalog"), "catalog": res.get("catalog"),
                  "format": sorted(map(str, c.formats)), "bit_processes": c.bit_processes,
                  "menu": {"name": (c.menu or {}).get("name"), "process": (c.menu or {}).get("process"),
                           "names_seen": c.menu_names},
                  "weight_branch": next(iter(c.weight_branches)) if len(c.weight_branches) == 1 else None,
                  "n_files": len(c.inputs), "n_jobs": res.get("n_jobs"), "n_jobs_merged": len(c.jobs)},
        "complete": result.complete, "missing_jobs": result.missing_jobs,
        "events": {"read": c.events["read"], "selected": c.events["selected"], "sumw": c.events["sumw"],
                   "sumw2": c.events["sumw2"]},
        "normalization": norm.to_dict(),
        "merged_from": "counters" if from_counters else "combination tables",
        "totals": totals,
        "checks": checks,
        "maps": {"source": maps.source,
                 "pure_references": ({g: REFERENCE_LABELS.get(g, g) for g in list(HLT_REFERENCES) + [r["pure_reference"] for r in l1]
                                      if g in REFERENCE_LABELS} if not from_counters else
                                     {"counters": "count-time definition of the counters (see settings.pure_reference)"}),
                 "n_streams": count_by(streams, "category"), "n_datasets": count_by(datasets, "category"),
                 "n_streams_split": len(maps.streams), "n_datasets_split": len(maps.datasets),
                 "n_paths": count_by([p for p in paths if p["kind"] == "hlt"], "category"),
                 "n_l1_bits": len(l1), "n_excluded": int(maps.excluded.sum()),
                 "split_datasets": {c: m for c, m in maps.combined_datasets.items() if len(m) > 1},
                 "split_streams": {c: m for c, m in maps.combined_streams.items() if len(m) > 1},
                 "split_dataset_methods": count_by(datasets_split, "method") if datasets_split else {},
                 "dataset_prescale_column": maps.prescale_column,
                 "stream_categories": {s: maps.stream_category[s] for s in maps.combined_streams}},
        "exclude_patterns": maps.exclude_patterns,
        "excluded_paths": [p["name"] for p in excluded],
        "paths_without_dataset": [p["name"] for p in without],
        "settings": result.settings,
        "warnings": result.warnings,
        "merge": result.timing,
    }

    docs = {"summary.json": summary,
            "paths_hlt.json": {**head("paths_hlt"), "menu_prescale_column": ps_column, "records": paths},
            "paths_l1.json": {**head("paths_l1"), "records": l1}}
    docs["excluded_paths.json"] = {
        **head("excluded_paths"), "patterns": maps.exclude_patterns, "default_list": DEFAULT_EXCLUDE, "records": excluded,
        "note": "Bits matched by merge.exclude or by the default list (config.DEFAULT_EXCLUDE; off with "
                "merge.default_exclude: false). They keep their own rate but are left out of the totals, the "
                "dataset and stream rates and the pure-rate references."}
    if maps.datasets and sums.datasets is not None:
        docs["datasets.json"] = {**head("datasets"), "records": datasets,
                                 "note": "combined datasets (split copies together); written_rate_hz = sum of "
                                         "the split datasets (datasets_split.json)"}
        docs["streams.json"] = {**head("streams"), "records": streams,
                                "note": "combined streams (split copies together); written_rate_hz = sum of "
                                        "the split streams (streams_split.json)"}
        docs["datasets_split.json"] = {**head("datasets_split"), "methods": DATASET_METHODS,
                                       "dataset_prescale_column": maps.prescale_column, "records": datasets_split}
        docs["streams_split.json"] = {**head("streams_split"), "methods": STREAM_METHODS, "records": streams_split}
        docs["paths_without_dataset.json"] = {
            **head("paths_without_dataset"), "records": without,
            "note": "HLT paths in no dataset of the menu (Dataset_ paths and L1 bits are not listed). They are "
                    "in no category total and have no pure rate; add them to merge.exclude to mark them."}
    if result.accumulator is not None:
        for ov in result.accumulator.overlaps:
            docs[f"overlaps_{ov.name}.json"] = overlap_document(result, ov, head(f"overlaps_{ov.name}"))

    csvs = {"paths_hlt.csv": _csv(paths, ["name", "category", "rate_hz", "rate_err_hz", "pure_rate_hz",
                                          "pure_rate_err_hz", "count", "pure_count", "pure_reference", "excluded"]
                                  + (["menu_prescale"] if ps_column else [])
                                  + ["shared_with_standard_parking_rate_hz", "shared_with_standard_parking_fraction",
                                     "datasets", "streams"]),
            "paths_l1.csv": _csv(l1, ["name", "seed", "flavour", "rate_hz", "rate_err_hz", "pure_rate_hz",
                                      "pure_rate_err_hz", "count", "pure_count", "pure_reference", "excluded"])}
    if "datasets.json" in docs:
        common = ["name", "category", "rate_hz", "rate_err_hz", "pure_rate_hz", "pure_rate_err_hz", "count",
                  "pure_count", "pure_reference", "shared_with_standard_parking_rate_hz",
                  "shared_with_standard_parking_fraction", "written_rate_hz", "n_split", "split", "n_paths"]
        csvs["datasets.csv"] = _csv(datasets, common + ["streams", "paths_missing"])
        csvs["streams.csv"] = _csv(streams, common + ["datasets"])
        csvs["datasets_split.csv"] = _csv(datasets_split, ["name", "combined", "category", "rate_hz", "rate_err_hz",
                                                           "count", "method", "dataset_prescale", "n_paths", "streams"])
        csvs["streams_split.csv"] = _csv(streams_split, ["name", "combined", "category", "rate_hz", "rate_err_hz",
                                                         "count", "method", "datasets"])
    return docs, csvs


def _dataset_records(result, sums, scale, pure_ref):
    """Records of the combined and split datasets and streams, and the split check."""
    maps = result.maps
    bit_index = {b: i for i, b in enumerate(maps.bits)}
    dp_bits = maps.dataset_path_bits()
    ps_table = bool(maps.dataset_prescales)

    def scaled(s3, f):
        return np.array([s3[0] * f, s3[1] * f, s3[2] * f * f])

    # split datasets: exact from the event-level decisions, or estimated from the OR of their paths
    split_ds = {}
    for j, d in enumerate(maps.datasets):
        c = maps.dataset_combined[d]
        n_copies = len(maps.combined_datasets[c])
        ps = maps.dataset_prescales.get(d) if ps_table else None
        if dp_bits[j] >= 0:
            method, f = "dataset_path", 1.0
        elif ps_table and ps != 1:
            method, f = "paths_div_prescale", (1.0 / ps if ps else 0.0)
        elif not ps_table and n_copies > 1:
            method, f = "paths_div_copies", 1.0 / n_copies
        else:
            method, f = "paths", 1.0
        present = [p for p in maps.datasets[d] if p in bit_index and not maps.excluded[bit_index[p]]]
        split_ds[d] = {"name": d, "combined": c, "category": maps.dataset_category[d],
                       "streams": maps.dataset_streams[d], "n_paths": len(present),
                       **rate_fields(scaled(sums.datasets_split[j], f), scale), "method": method,
                       "dataset_prescale": ps, "_s3": scaled(sums.datasets_split[j], f)}

    split_st, upper = {}, []
    for k, s in enumerate(maps.streams):
        dss = [d for d in maps.streams[s] if d in split_ds]
        exact = all(split_ds[d]["method"] in EXACT_DATASET_METHODS for d in dss)
        if exact:
            method, s3 = "datasets", sums.streams_split[k]
        elif len({maps.dataset_combined[d] for d in dss}) == 1:
            # copies of one combined dataset never share an event: their estimates add up (they all
            # come from the same OR of paths, so their uncertainties add linearly)
            method = "sum_of_datasets"
            s3 = np.array([sum(split_ds[d]["_s3"][0] for d in dss), sum(split_ds[d]["_s3"][1] for d in dss),
                           sum(math.sqrt(max(split_ds[d]["_s3"][2], 0.0)) for d in dss) ** 2])
        else:
            method, s3 = "upper_bound", sums.streams_split[k]
            upper.append(s)
        split_st[s] = {"name": s, "combined": maps.stream_combined[s], "category": maps.stream_category[s],
                       "datasets": maps.streams[s], **rate_fields(s3, scale), "method": method, "_s3": s3,
                       "_exact": exact, "_from_bits": exact and any(split_ds[d]["method"] == "dataset_path" for d in dss)}
    if upper:
        result.warnings.append(f"{len(upper)} split stream(s) hold datasets with different paths and a dataset "
                               f"prescale but no Dataset_ bit: their rate is an upper bound (OR of the paths): {upper[:5]}")

    def written(members, recs, exact):
        """Sum of the split items' (count, Σw, Σw²): independent if exact (they never share an event),
        fully correlated if estimated (all from the same OR of paths)."""
        s3 = [recs[m]["_s3"] for m in members]
        if exact:
            return np.sum(s3, axis=0)
        return np.array([sum(x[0] for x in s3), sum(x[1] for x in s3), sum(math.sqrt(max(x[2], 0.0)) for x in s3) ** 2])

    def combined_record(level, j, name, members, recs, exact):
        fired = getattr(sums, level)
        cat = maps.dataset_category[name] if level == "datasets" else maps.stream_category[name]
        rec = {"name": name, "category": cat, "n_split": len(members), "split": members}
        rec.update(rate_fields(fired["fired"][j], scale))
        group = maps.group_reference(cat)
        rec.update({"pure_reference": pure_ref(group), **rate_fields(fired["pure"][j], scale, "pure_")})
        if group == "scouting":
            rec.update(shared_fields(fired["shared_sp"][j], fired["fired"][j], scale))
        rec.update(rate_fields(written(members, recs, exact), scale, "written_"))
        return rec

    datasets, mismatches, checked = [], [], 0
    for j, (c, members) in enumerate(maps.combined_datasets.items()):
        paths = list(dict.fromkeys(p for d in members for p in maps.datasets[d]))
        present = [p for p in paths if p in bit_index]
        exact = all(split_ds[d]["method"] in EXACT_DATASET_METHODS for d in members)
        rec = combined_record("datasets", j, c, members, split_ds, exact)
        rec.update({"streams": maps.combined_dataset_streams(c),
                    "n_paths": sum(not maps.excluded[bit_index[p]] for p in present),
                    "paths": [p for p in present if not maps.excluded[bit_index[p]]],
                    "paths_excluded": [p for p in present if maps.excluded[bit_index[p]]],
                    "paths_missing": [p for p in paths if p not in bit_index]})
        if len(members) > 1 and all(split_ds[d]["method"] == "dataset_path" for d in members):
            checked += 1
            if rec["written_count"] != rec["count"]:
                mismatches.append(f"{c}: {rec['written_count']} in {'+'.join(members)}, {rec['count']} combined")
        datasets.append(rec)
    dataset_paths = {r["name"]: r["paths"] for r in datasets}
    streams = []
    for k, (c, members) in enumerate(maps.combined_streams.items()):
        exact = all(split_st[s]["_exact"] for s in members)
        rec = combined_record("streams", k, c, members, split_st, exact)
        dss = maps.combined_stream_datasets(c)
        rec.update({"datasets": dss, "n_paths": len({p for d in dss for p in dataset_paths[d]})})
        if len(members) > 1 and all(split_st[s]["_from_bits"] for s in members):
            checked += 1
            if rec["written_count"] != rec["count"]:
                mismatches.append(f"{c}: {rec['written_count']} in {'+'.join(members)}, {rec['count']} combined")
        streams.append(rec)

    check = {"result": ("MISMATCH" if mismatches else "ok") if checked else "not checked (no Dataset_ bits)",
             "description": "split datasets/streams decided by their Dataset_ bits add up to the combined one",
             "checked": checked, "mismatches": mismatches}
    strip = lambda recs: [{k: v for k, v in r.items() if not k.startswith("_")} for r in recs]   # noqa: E731
    return (_by_rate(datasets), _by_rate(streams), _by_rate(strip(split_ds.values())),
            _by_rate(strip(split_st.values())), check)


def _input_from_generated(totals, norm, collected):
    """MC ntuples that store only some events (e.g. only triggered ones): the HLT input is all
    generated events (Runs-tree sums), not the stored ones. ``input`` then describes the generated
    events (exact by construction: σ x L or the collision rate, so no uncertainty), ``stored`` the
    events in the ntuples, and every ``fraction_of_input`` refers to the generated events."""
    if norm.mode == "data" or norm.details.get("fraction_kept_in_ntuples") is None:
        return
    totals["stored"] = dict(totals["input"], description="events stored in the ntuples")
    gen_count = sum((r.get("mc_sums") or {}).get("genEventCount", 0.0) for r in collected.inputs)
    rate = norm.details["sumw_total"] * norm.scale
    totals["input"] = {"description": "all generated events (HLT input), from the Runs-tree sums",
                       "count": int(round(gen_count)), "rate_hz": sig(rate), "rate_err_hz": 0.0}
    for t in totals.values():
        t["fraction_of_input"] = sig(t["rate_hz"] / rate) if rate else 0.0


def _csv(records, columns):
    rows = []
    for r in records:
        row = []
        for col in columns:
            v = r.get(col, "")
            row.append(" ".join(v) if isinstance(v, list) else ("" if v is None else v))
        rows.append(row)
    return columns, rows


def overlap_document(result, ov, head):
    acc, maps = result.accumulator, result.maps
    labels = {"bits": maps.bits, "datasets": list(maps.combined_datasets), "streams": list(maps.combined_streams)}
    rows = [n for n, m in zip(labels[ov.rows[0]], ov.rows[1]) if m]
    cols = [n for n, m in zip(labels[ov.cols[0]], ov.cols[1]) if m]
    count, sumw, sumw2 = acc.overlap_matrix(ov.name)
    n_cols = count.shape[1]

    def entries_of(m):
        """(sorted linear keys row * n_cols + col, values) of the stored entries of a sparse matrix."""
        m = sp.coo_matrix(m)
        keys = m.row.astype(np.int64) * n_cols + m.col
        order = np.argsort(keys, kind="stable")
        return keys[order], m.data[order]

    # entries = cells with at least one event (count > 0); Σw may cancel to 0 with negative weights,
    # so Σw and Σw² are looked up at the cells of the count matrix (0 where they are not stored)
    keys, cnt = entries_of(count)
    keep = cnt != 0
    if ov.symmetric:
        keep &= (keys // n_cols) <= (keys % n_cols)
    keys, cnt = keys[keep], cnt[keep]

    def values_at(m):
        k, v = entries_of(m)
        out = np.zeros(len(keys))
        pos = np.searchsorted(k, keys)
        found = pos < len(k)
        found[found] = k[pos[found]] == keys[found]
        out[found] = v[pos[found]]
        return out

    scale = result.normalization.scale
    rates = values_at(sumw) * scale
    errs = np.sqrt(np.clip(values_at(sumw2), 0.0, None)) * scale
    entries = [[int(k // n_cols), int(k % n_cols), int(round(c)), sig(r), sig(e)]
               for k, c, r, e in zip(keys.tolist(), cnt.tolist(), rates.tolist(), errs.tolist())]
    return {**head, "name": ov.name, "description": ov.description, "symmetric": ov.symmetric,
            "rows": rows, "cols": cols, "entry_columns": ["row", "col", "count", "rate_hz", "rate_err_hz"],
            "entries": entries}


# --------------------------------------------------------------------------------------------------
# Writing
# --------------------------------------------------------------------------------------------------

def _write_atomic(path, write):
    tmp = f"{path}.tmp-{os.getpid()}"
    with open(tmp, "w", newline="") as f:
        write(f)
    os.replace(tmp, path)


def write_results(result, out_dir):
    """Write all output files; removes outputs of an earlier merge that this one does not produce.
    Returns the summary document."""
    docs, csvs = build_documents(result)
    os.makedirs(out_dir, exist_ok=True)
    written = sorted(docs) + sorted(csvs)
    docs["summary.json"]["files"] = written
    for fname, doc in docs.items():
        compact = fname.startswith("overlaps_")
        _write_atomic(os.path.join(out_dir, fname),
                      lambda f, doc=doc, compact=compact: (json.dump(doc, f, indent=None if compact else 1,
                                                                     separators=(",", ":") if compact else None),
                                                           f.write("\n")))
    for fname, (header, rows) in csvs.items():
        def write(f, header=header, rows=rows):
            w = csv.writer(f)
            w.writerow(header)
            w.writerows(rows)
        _write_atomic(os.path.join(out_dir, fname), write)
    for fname in KNOWN_FILES:
        if fname not in written and os.path.exists(os.path.join(out_dir, fname)):
            os.remove(os.path.join(out_dir, fname))
    return docs["summary.json"]


def print_summary(result, summary, out_dir, top=10):
    n = summary["normalization"]
    ev = summary["events"]
    t = summary["totals"]
    lines = [f"Merged {result.workdir}: {summary['merge']['partials']} partials, {ev['selected']:,} events"
             + ("" if summary["complete"] else f"  ** INCOMPLETE: {len(summary['missing_jobs'])} jobs missing **"),
             f"  merge: {summary['merge']['runtime_s']} s, max RSS {summary['merge']['max_rss_mb']} MB, from the "
             + summary["merged_from"]]
    if n["mode"] == "data":
        lines.append(f"  normalization: data, prescale {n['prescale']:g}, N_LS {n['n_ls']}, lumi {n['lumi']:g} -> "
                     f"{n['target_lumi']:g}: {n['scale_hz_per_unit_weight']:.6g} Hz per event")
    else:
        lines.append(f"  normalization: {n['mode']}, sum of weights {n['sumw_total']:.6g} ({n['sumw_total_source']}): "
                     f"{n['scale_hz_per_unit_weight']:.6g} Hz per unit weight")
    order = ["input", "stored", "physics", "standard", "parking", "scouting", "other", "all_datasets",
             "scouting_and_standard_parking", "l1", "l1_final", "l1_beforePrescale"]
    shown = [k for k in order if k in t] + [k for k in t if k.startswith("regex_")]
    lines.append("  totals [Hz]: " + ", ".join(f"{k} {t[k]['rate_hz']:.6g}" for k in shown))
    for name, chk in summary["checks"].items():
        if "result" in chk:
            lines.append(f"  check {name}: {chk['result']}")
    docs_paths = [p for p in json.load(open(os.path.join(out_dir, "paths_hlt.json")))["records"] if p["kind"] == "hlt"]
    lines.append(f"  top {top} HLT paths [Hz]: " + ", ".join(f"{p['name']} {p['rate_hz']:.4g}" for p in docs_paths[:top]))
    excl = summary["excluded_paths"]
    lines.append(f"  excluded from all combined quantities: {len(excl)} bit(s)"
                 + (f": {', '.join(excl)}" if 0 < len(excl) <= 20 else "") + " (excluded_paths.json)")
    for w in summary["warnings"]:
        lines.append(f"  WARNING: {w}")
    lines.append(f"  results: {out_dir}/ ({len(summary['files'])} files)")
    print("\n".join(lines))
