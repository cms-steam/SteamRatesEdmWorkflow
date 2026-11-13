"""Old-style counters vs brute force and vs the combination table; local jobs and their status."""

import json
import os

import numpy as np
import pytest

from steamrates import count
from steamrates.batch import submit
from steamrates.counters import BitCounters
from steamrates.jobstatus import job_states
from steamrates.partial import PartialResult
from steamrates.patterns import PatternAccumulator
from steamrates.prepare import prepare

NAMES = ["HLT_A_v1", "HLT_B_v2", "HLT_C_v1", "HLT_Physics_v3", "L1_X", "L1_Y", "L1_FinalOR", "DST_Z_v1"]
EXCLUDE = ["^HLT_Physics$"]
TOTALS = {"HLT": "^HLT_", "L1": "^L1_(?!FinalOR)"}


def random_chunks(rng, weighted):
    """Three chunks; the second has other bits in another order (like a different menu)."""
    layouts = [NAMES, ["L1_Y", "HLT_B_v2", "HLT_New_v1", "HLT_A_v1"], NAMES]
    chunks = []
    for names in layouts:
        n = 3000
        bits = rng.random((n, len(names))) < rng.uniform(0.02, 0.4, len(names))
        w = rng.normal(1.0, 2.0, n) if weighted else None          # includes negative weights
        chunks.append((names, bits, w))
    return chunks


def brute_force(chunks):
    """Expected counters from one global event x bit matrix."""
    names = []
    for chunk_names, _, _ in chunks:
        names += [b for b in chunk_names if b not in names]
    rows, weights = [], []
    for chunk_names, bits, w in chunks:
        full = np.zeros((len(bits), len(names)), bool)
        for j, b in enumerate(chunk_names):
            full[:, names.index(b)] = bits[:, j]
        rows.append(full)
        weights.append(np.ones(len(bits)) if w is None else w)
    x, w = np.vstack(rows), np.concatenate(weights)
    base = [b.rsplit("_v", 1)[0] if "_v" in b else b for b in names]
    excluded = np.array([b == "HLT_Physics" for b in base])
    ref = np.array([b.startswith("HLT_") for b in base]) & ~excluded
    n_ref = x[:, ref].sum(axis=1)
    pure = x & np.where(ref, (n_ref == 1)[:, None], (n_ref == 0)[:, None])
    exp = {"count": x.sum(0), "sumw": w @ x, "sumw2": (w * w) @ x,
           "pure_count": pure.sum(0), "pure_sumw": w @ pure, "pure_sumw2": (w * w) @ pure}
    totals = {}
    for label, prefix in (("HLT", "HLT_"), ("L1", "L1_")):
        m = np.array([b.startswith(prefix) and b != "L1_FinalOR" for b in base]) & ~excluded
        hit = x[:, m].any(axis=1)
        totals[label] = (hit.sum(), w[hit].sum(), (w[hit] ** 2).sum())
    return names, exp, totals, ref


@pytest.mark.parametrize("weighted", [False, True])
def test_counters_match_brute_force_and_table(weighted):
    rng = np.random.default_rng(7)
    chunks = random_chunks(rng, weighted)
    counters = BitCounters("^HLT_", EXCLUDE, TOTALS)
    acc = PatternAccumulator()
    for names, bits, w in chunks:
        counters.add(names, bits, w)
        acc.add(names, bits, w)
    names, exp, totals, ref = brute_force(chunks)
    got = counters.arrays(names)
    for field, expected in exp.items():
        np.testing.assert_allclose(got[field], expected, rtol=1e-12, atol=1e-9, err_msg=field)
    definition = counters.definition()
    for label, (n, sw, sw2) in totals.items():
        t = definition["totals"][label]
        assert t["count"] == n
        assert t["sumw"] == pytest.approx(sw) and t["sumw2"] == pytest.approx(sw2)

    # the combination table gives the same numbers
    table = acc.result()
    order = [table.bit_names.index(b) for b in names]
    ref_t = np.zeros(table.n_bits, bool)
    ref_t[order] = ref
    for what in ("count", "sumw", "sumw2"):
        np.testing.assert_allclose(table.bit_totals(what)[order], exp[what], rtol=1e-12, atol=1e-9)
        np.testing.assert_allclose(table.exclusive_totals(ref_t, what, non_reference="exclusive")[order],
                                   exp["pure_" + what], rtol=1e-12, atol=1e-9)
    hlt = np.array([b.startswith("HLT_") and not counters.is_excluded(b) for b in table.bit_names])
    assert table.any_totals(hlt, "count") == totals["HLT"][0]


def test_count_writes_counters_and_no_table(catalog_dir, tmp_path):
    files = [str(tmp_path / "data" / f"d{i}.root") for i in range(2)]
    full, light = str(tmp_path / "full.root"), str(tmp_path / "light.root")
    assert count.main(["-o", full, "--exclude", "^HLT_B$", "--"] + files) == 0
    assert count.main(["-o", light, "--exclude", "^HLT_B$", "--no-table", "--"] + files) == 0
    a, b = PartialResult.read(full), PartialResult.read(light)
    assert a.table is not None and b.table is None and b.metadata["has_table"] is False
    assert a.metadata["counters"] == b.metadata["counters"]
    order = [b.bit_names.index(n) for n in a.bit_names]
    for field in a.counters:
        np.testing.assert_array_equal(a.counters[field], b.counters[field][order])
    np.testing.assert_array_equal(a.counters["count"], a.table.bit_totals("count"))
    assert b.metadata["totals"]["n_events"] == a.metadata["totals"]["n_events"] == 200


def test_local_jobs_status_and_resubmit(catalog_dir, write_config, tmp_path, capsys):
    config = write_config("name: m\ninput: {ntuples: DataNano}\njobs: {events_per_job: 100}\n")
    wd, _ = prepare(config, str(tmp_path / "wd"), catalog_dir=catalog_dir)
    states = submit(wd, n_parallel=2)
    assert [s for _, s, _ in states] == ["done"] * 4
    meta = PartialResult.read(wd.partial(2)).metadata
    assert meta["job"]["id"] == 2 and meta["job"]["key"] == wd.jobs()[2]["key"]

    # a deleted partial is missing, a corrupt one invalid; submit reruns exactly those
    os.remove(wd.partial(1))
    open(wd.partial(3), "w").write("garbage")
    states = {j["id"]: s for j, s, _ in job_states(wd)}
    assert states == {0: "done", 1: "missing", 2: "done", 3: "invalid"}
    mtime0 = os.path.getmtime(wd.partial(0))
    capsys.readouterr()
    states = submit(wd, n_parallel=2)
    assert "running 2 of 4 jobs" in capsys.readouterr().out
    assert [s for _, s, _ in states] == ["done"] * 4 and os.path.getmtime(wd.partial(0)) == mtime0

    # a job that cannot read its input fails, with the log tail in the status
    job_file = wd.job_json(0)
    job = json.load(open(job_file))
    job["files"] = [str(tmp_path / "does_not_exist.root")]
    json.dump(job, open(job_file, "w"))
    os.remove(wd.partial(0))
    states = submit(wd, n_parallel=1)
    state, detail = states[0][1], states[0][2]
    assert state == "failed" and "exit code" in detail
    assert "does_not_exist.root" in open(wd.p("logs", "job_0000.log")).read()
