"""The merge step end to end on synthetic work dirs: normalization, outputs, refusals, cross-checks."""

import json
import math
import os

import numpy as np
import pytest
import uproot

from steamrates.batch import submit
from steamrates.merge import MergeError, merge
from steamrates.normalize import LS_LENGTH_S
from steamrates.partial import PartialResult
from steamrates.prepare import prepare
from steamrates.results import write_results


def run_measurement(config, workdir, catalog_dir):
    wd, _ = prepare(config, workdir, catalog_dir=catalog_dir)
    assert all(s == "done" for _, s, _ in submit(wd, n_parallel=2))
    return wd


def read_bits(paths, lumi_max=None):
    """Brute force: the trigger bits of the synthetic files, events with lumi <= lumi_max."""
    out = []
    for p in paths:
        ev = uproot.open(p)["Events"].arrays(library="np")
        keep = np.ones(len(ev["run"]), bool) if lumi_max is None else ev["luminosityBlock"] <= lumi_max
        out.append({k: v[keep] for k, v in ev.items()})
    return {k: np.concatenate([o[k] for o in out]) for k in out[0]}


def records(wd, name):
    return {r["name"]: r for r in json.load(open(wd.p("results", name)))["records"]}


def test_data_merge_with_dataset_files(catalog_dir, write_config, tmp_path):
    (tmp_path / "ds.yaml").write_text("DA: [HLT_A]\nDB: [HLT_B, HLT_Gone]\n")
    (tmp_path / "st.yaml").write_text("PhysicsX: [DA]\nScoutingY: [DB]\n")
    config = write_config("""\
        name: m
        input: {ntuples: DataNano}
        jobs: {events_per_job: 100}
        merge: {datasets: ds.yaml, streams: st.yaml}
        """)
    wd = run_measurement(config, str(tmp_path / "wd"), catalog_dir)
    result = merge(wd.path)
    summary = write_results(result, wd.p("results"))

    files = sorted(str(p) for p in (tmp_path / "data").glob("*.root"))
    ev = read_bits(files, lumi_max=6)                      # golden JSON: run 1, lumi 1-6
    n = len(ev["run"])
    scale = 100 / (6 * LS_LENGTH_S)                        # prescale 100, lumi 2.0 -> 2.0, 6 lumi sections
    assert summary["normalization"]["n_ls"] == 6 and math.isclose(summary["normalization"]["scale_hz_per_unit_weight"], scale)
    assert summary["events"]["selected"] == n and summary["complete"] is True
    paths = records(wd, "paths_hlt.json")
    a, b = ev["HLT_A"], ev["HLT_B"]
    assert paths["HLT_A"]["count"] == a.sum()
    assert math.isclose(paths["HLT_A"]["rate_hz"], a.sum() * scale, rel_tol=1e-5)
    assert math.isclose(paths["HLT_A"]["rate_err_hz"], math.sqrt(a.sum()) * scale, rel_tol=1e-5)
    # categories from the stream names: HLT_A standard, HLT_B scouting -> each pure w.r.t. its own group
    assert (paths["HLT_A"]["category"], paths["HLT_B"]["category"]) == ("standard", "scouting")
    assert paths["HLT_A"]["pure_count"] == a.sum() and paths["HLT_B"]["pure_count"] == b.sum()
    assert paths["HLT_B"]["shared_with_standard_parking_rate_hz"] == pytest.approx((a & b).sum() * scale, rel=1e-5)
    datasets = records(wd, "datasets.json")
    assert datasets["DB"]["paths_missing"] == ["HLT_Gone"] and datasets["DA"]["count"] == a.sum()
    streams = records(wd, "streams.json")
    assert streams["ScoutingY"]["shared_with_standard_parking_fraction"] == pytest.approx((a & b).sum() / b.sum(), rel=1e-5)
    t = summary["totals"]
    assert t["input"]["count"] == n and t["physics"]["count"] == (a | b).sum()
    assert t["scouting_and_standard_parking"]["count"] == (a & b).sum()
    assert t["l1"]["count"] == ev["L1_X"].sum()
    ov = json.load(open(wd.p("results", "overlaps_datasets.json")))
    cells = {(ov["rows"][i], ov["cols"][j]): c for i, j, c, _, _ in ov["entries"]}
    assert cells[("DA", "DB")] == (a & b).sum() and cells[("DA", "DA")] == a.sum() and ("DB", "DA") not in cells
    assert summary["checks"]["counters_vs_table"]["result"] == "ok"
    assert os.path.exists(wd.p("results", "paths_hlt.csv")) and os.path.exists(wd.p("results", "paths_without_dataset.json"))
    split = records(wd, "datasets_split.json")             # no menu: no prescales, nothing split
    assert split["DA"]["method"] == "paths" and split["DA"]["count"] == a.sum() and split["DA"]["combined"] == "DA"
    assert os.path.exists(wd.p("results", "datasets_split.csv")) and os.path.exists(wd.p("results", "streams_split.csv"))

    # the counters give the same path rates
    counters = write_results(merge(wd.path, from_counters=True), str(tmp_path / "from_counters"))
    assert counters["merged_from"] == "counters"
    cpaths = {r["name"]: r for r in json.load(open(tmp_path / "from_counters" / "paths_hlt.json"))["records"]}
    assert cpaths["HLT_A"]["rate_hz"] == paths["HLT_A"]["rate_hz"]
    assert not (tmp_path / "from_counters" / "datasets.json").exists()


def test_missing_jobs_and_tampered_counters(catalog_dir, write_config, tmp_path):
    config = write_config("name: m\ninput: {ntuples: DataNano}\njobs: {events_per_job: 100}\n")
    wd = run_measurement(config, str(tmp_path / "wd"), catalog_dir)
    os.remove(wd.partial(3))
    with pytest.raises(MergeError, match="1 of 4 jobs are not done"):
        merge(wd.path)
    result = merge(wd.path, allow_missing=True)
    assert result.complete is False and result.missing_jobs == [3]
    assert any("INCOMPLETE" in w for w in result.warnings)

    p = PartialResult.read(wd.partial(0))
    p.counters["count"][0] += 1                              # the event loop and the table disagree
    p.write(wd.partial(0))
    with pytest.raises(MergeError, match="do not reproduce the counters"):
        merge(wd.path, allow_missing=True)


def test_mc_cross_section(catalog_dir, write_config, tmp_path):
    config = write_config("name: mc\ninput: {ntuples: McNano}\njobs: {events_per_job: 100}\n"
                          "merge: {exclude: ['^HLT_B$']}\n")
    wd = run_measurement(config, str(tmp_path / "wd"), catalog_dir)
    summary = write_results(merge(wd.path), wd.p("results"))
    # 50 pb x 0.5 x 7.5e34 = 1.875 Hz for all 60 events (no Runs-tree sums: the counted events)
    assert summary["totals"]["input"]["rate_hz"] == pytest.approx(50 * 0.5 * 7.5 * 0.01)
    assert summary["normalization"]["sumw_total"] == 60
    assert any("no genEventCount" in w for w in summary["warnings"])
    # excluded: listed with the matching pattern, left out of the totals, also in the counters
    excluded = records(wd, "excluded_paths.json")
    assert list(excluded) == ["HLT_B"] and excluded["HLT_B"]["patterns"] == ["^HLT_B$"]
    assert excluded["HLT_B"]["default_list"] is False and summary["excluded_paths"] == ["HLT_B"]
    ev = read_bits(sorted(str(p) for p in (tmp_path / "mc").glob("*.root")))
    assert summary["totals"]["regex_HLT"]["count"] == ev["HLT_A"].sum()
    assert summary["checks"]["counters_vs_table"]["result"] == "ok"
    assert "^HLT_B$" in PartialResult.read(wd.partial(0), table=False).metadata["counters"]["exclude"]
