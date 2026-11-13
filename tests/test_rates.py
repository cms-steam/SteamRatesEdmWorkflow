"""Maps, normalization and the rate accumulator vs brute force on random events."""

import math
from types import SimpleNamespace

import numpy as np
import pytest

from steamrates import normalize
from steamrates.config import DEFAULT_EXCLUDE
from steamrates.maps import build_maps, combine_split, stream_category
from steamrates.patterns import PatternTable
from steamrates.rates import RateAccumulator, default_overlaps, default_totals
from steamrates.results import _dataset_records, overlap_document

BITS = ["HLTriggerFirstPath", "HLT_A_v1", "HLT_B_v2", "HLT_C_v1", "HLT_D_v1", "HLT_E_v3", "HLT_Excl_v1",
        "HLT_Mon_v1", "HLT_NoDs_v1", "DST_S1_v1", "DST_S2_v1", "AlCa_X_v1", "Dataset_D1",
        "L1_X_final", "L1_Y_final", "L1_X_beforePrescale", "L1_Y_beforePrescale", "L1_FinalOR"]
MENU = {
    "trigger_paths": [b for b in BITS if not b.startswith("L1")] + ["HLT_Missing_v1"],
    "datasets": {"D1": ["HLT_A_v1", "HLT_B_v2", "HLT_Excl_v1"], "D2": ["HLT_B_v2", "HLT_C_v1"],
                 "P1": ["HLT_D_v1"], "P2": ["HLT_E_v3", "HLT_Missing_v1"],
                 "S1": ["DST_S1_v1", "DST_S2_v1", "HLT_A_v1"], "M1": ["HLT_Mon_v1", "HLT_A_v1", "AlCa_X_v1"]},
    "streams": {"PhysicsA": ["D1", "D2"], "ParkingB": ["P1", "P2"], "ScoutingC": ["S1"], "Express": ["M1"]},
}
SETTINGS = {"strip_versions": True, "pure_reference": "^HLT_", "exclude": ["^HLT_Excl$"],
            "totals": {"HLT": "^HLT_"}, "datasets": "menu", "streams": "menu",
            "overlaps": {"datasets": True, "streams": True, "paths": True, "paths_datasets": True,
                         "l1_datasets": True, "l1_streams": True, "l1": True, "paths_l1": True}}
STRIPPED = [b.rsplit("_v", 1)[0] if "_v" in b else b for b in BITS]


def test_categories_and_references():
    assert [stream_category(s) for s in ("PhysicsEGamma0", "ParkingVBF1", "ScoutingPF0", "Express", "NanoDST")] \
        == ["standard", "parking", "scouting", "other", "other"]
    m = build_maps(STRIPPED, MENU, SETTINGS)
    cat = {b: m.main_category(i) for i, b in enumerate(m.bits)}
    assert cat["HLT_A"] == "standard" and m.path_categories[m.bits.index("HLT_A")] == ["standard", "scouting", "other"]
    assert (cat["HLT_D"], cat["DST_S1"], cat["HLT_Mon"], cat["HLT_NoDs"], cat["Dataset_D1"], cat["L1_X_final"]) \
        == ("parking", "scouting", "other", "none", "dataset_path", "l1")
    ref = {b: m.bit_reference(i) for i, b in enumerate(m.bits)}
    assert ref["HLT_A"] == "standard_parking" and ref["DST_S1"] == "scouting"
    assert ref["HLT_Mon"] is None and ref["HLT_Excl"] is None and ref["L1_FinalOR"] is None
    assert ref["L1_X_beforePrescale"] == "l1_beforePrescale"
    assert m.missing_paths("P2") == ["HLT_Missing"]
    assert [m.bits[i] for i in m.paths_without_dataset()] == ["HLTriggerFirstPath", "HLT_NoDs"]
    assert m.bits_not_in_menu() == []


def test_maps_without_datasets_warn():
    m = build_maps(STRIPPED, None, SETTINGS)
    assert m.main_category(m.bits.index("HLT_C")) == "standard"
    assert m.main_category(m.bits.index("DST_S1")) == "none"
    assert any("no datasets" in w for w in m.warnings)


def test_maps_without_streams_scouting_by_name():
    m = build_maps(["HLT_A", "DST_PFScouting", "MC_X"], None, dict(SETTINGS, scouting_paths="^DST_.*Scouting"))
    assert [m.main_category(i) for i in range(3)] == ["standard", "scouting", "none"]
    assert m.bit_reference(1) == "scouting"
    labels = [t.label for t in default_totals(m, {})]
    assert {"standard", "scouting", "physics", "scouting_and_standard_parking"} <= set(labels)


def test_default_exclude_list():
    bits = ["HLT_Physics", "HLT_PhysicsX", "HLT_EphemeralPhysics", "HLT_EphemeralZeroBias", "HLT_Y"]
    m = build_maps(bits, None, dict(SETTINGS, exclude=["^HLT_Y$"]))
    assert m.excluded_by == {"HLT_Physics": ["^HLT_Physics$"], "HLT_EphemeralPhysics": ["^HLT_EphemeralPhysics$"],
                             "HLT_Y": ["^HLT_Y$"]}
    assert m.exclude_patterns == DEFAULT_EXCLUDE + ["^HLT_Y$"]
    m = build_maps(bits, None, dict(SETTINGS, exclude=["^HLT_Y$"], default_exclude=False))
    assert list(m.excluded_by) == ["HLT_Y"]


def test_combine_split():
    content = {"E0": "a", "E1": "a", "E2": "a", "E10": "a", "AlCaP0": "p", "T0": "x", "T1": "y",
               "TestDataRaw": "t", "TestDataScouting": "t", "Q1": "q", "Q2": "q", "Q": "z"}
    groups, warnings = combine_split(sorted(content), content.get)
    assert groups["E"] == ["E0", "E1", "E2", "E10"]                    # in the order of their numbers
    assert groups["AlCaP0"] == ["AlCaP0"]                              # a single one is not split
    assert groups["T0"] == ["T0"] and groups["T1"] == ["T1"]           # different content
    assert groups["TestDataRaw"] == ["TestDataRaw"]                    # same content, no number
    assert groups["Q1"] == ["Q1"] and groups["Q"] == ["Q"] and len(warnings) == 1   # "Q" exists itself


def random_events(rng, n, weighted):
    p = rng.uniform(0.02, 0.35, len(BITS))
    x = rng.random((n, len(BITS))) < p
    w = rng.normal(1.0, 1.5, n) if weighted else np.ones(n)
    return x, w


@pytest.mark.parametrize("weighted", [False, True])
def test_accumulator_matches_brute_force(weighted):
    rng = np.random.default_rng(7)
    x, w = random_events(rng, 4000, weighted)
    m = build_maps(STRIPPED, MENU, SETTINGS)
    acc = RateAccumulator(m, default_totals(m, SETTINGS["totals"]), default_overlaps(m, SETTINGS["overlaps"]), block=97)
    # two tables (two partials), the second with the bits in another order
    perm = rng.permutation(len(BITS))
    acc.add_table(PatternTable.from_bits(STRIPPED, x[:1500], None if not weighted else w[:1500]))
    acc.add_table(PatternTable.from_bits([STRIPPED[j] for j in perm], x[1500:][:, perm],
                                         None if not weighted else w[1500:]))
    W = np.column_stack([np.ones(len(w)), w, w * w])

    def close(a, b):
        np.testing.assert_allclose(a, b, rtol=1e-9, atol=1e-9)

    # per bit
    close(acc.sums["bits"]["fired"], x.T.astype(float) @ W)
    excl = np.array([b == "HLT_Excl" for b in STRIPPED])
    cats = {b: set(m.path_categories[i]) for i, b in enumerate(m.bits)}
    in_cat = lambda cs: np.array([bool(cats[b] & set(cs)) for b in STRIPPED]) & ~excl   # noqa: E731
    sp_ref, sc_ref = in_cat(["standard", "parking"]), in_cat(["scouting"])
    n_sp, n_sc = x[:, sp_ref].sum(1), x[:, sc_ref].sum(1)
    expected_pure = np.zeros((len(BITS), 3))
    for i, b in enumerate(STRIPPED):
        g = m.bit_reference(i)
        if g == "standard_parking":
            sel = x[:, i] & (n_sp == 1)
        elif g == "scouting":
            sel = x[:, i] & (n_sc == 1)
        elif g in ("l1_final", "l1_beforePrescale"):
            seeds = np.array([s.startswith("L1_") and s.endswith(g[3:]) and "FinalOR" not in s for s in STRIPPED])
            sel = x[:, i] & (x[:, seeds].sum(1) == 1)
        else:
            continue
        expected_pure[i] = W[sel].sum(0)
    close(acc.sums["bits"]["pure"], expected_pure)
    sp_any = x[:, sp_ref].any(1)
    close(acc.sums["bits"]["shared_sp"], (x & sp_any[:, None]).T.astype(float) @ W)

    # datasets and streams: OR of all member paths; pure rates: OR of the non-excluded ones
    ds_names, st_names = list(m.datasets), list(m.streams)

    def decisions(keep):
        g_ds = np.column_stack([x[:, [i for i, b in enumerate(STRIPPED) if b in m.datasets[d] and keep[i]]].any(1)
                                for d in ds_names])
        return g_ds, np.column_stack([g_ds[:, [ds_names.index(d) for d in m.streams[s]]].any(1) for s in st_names])
    g_ds, g_st = decisions(np.ones(len(STRIPPED), bool))
    r_ds, r_st = decisions(~excl)
    assert (g_ds != r_ds).any()                            # HLT_Excl is in D1
    close(acc.sums["datasets"]["fired"], g_ds.T.astype(float) @ W)
    close(acc.sums["streams"]["fired"], g_st.T.astype(float) @ W)
    for names, g, r, lvl, catmap in ((ds_names, g_ds, r_ds, "datasets", m.dataset_category),
                                     (st_names, g_st, r_st, "streams", m.stream_category)):
        sp_items = np.array([catmap[n] in ("standard", "parking") for n in names])
        sc_items = np.array([catmap[n] == "scouting" for n in names])
        exp = np.zeros((len(names), 3))
        for j, n in enumerate(names):
            ref = sp_items if sp_items[j] else sc_items if sc_items[j] else None
            if ref is not None:
                exp[j] = W[r[:, j] & (r[:, ref].sum(1) == 1)].sum(0)
        close(acc.sums[lvl]["pure"], exp)
        close(acc.sums[lvl]["shared_sp"], (g & sp_any[:, None]).T.astype(float) @ W)

    # totals
    t = acc.total_sums
    close(t["input"], W.sum(0))
    close(t["standard"], W[x[:, in_cat(["standard"])].any(1)].sum(0))
    close(t["physics"], W[x[:, in_cat(["standard", "parking", "scouting"])].any(1)].sum(0))
    close(t["scouting_and_standard_parking"], W[x[:, sc_ref].any(1) & sp_any].sum(0))
    hlt_rx = np.array([b.startswith("HLT_") for b in STRIPPED]) & ~excl
    close(t["regex_HLT"], W[x[:, hlt_rx].any(1)].sum(0))

    # overlaps (all three W columns); L1 x L1 and HLT x L1 with the _final seeds only
    hlt = np.array([k == "hlt" for k in m.kind])
    l1f = np.array([b.startswith("L1_") and b.endswith("_final") for b in STRIPPED])
    assert l1f.sum() == 2
    assert next(o for o in acc.overlaps if o.name == "l1").description == "L1 seed x L1 seed (final)"
    for name, a, b in (("datasets", g_ds, g_ds), ("streams", g_st, g_st), ("paths_datasets", x[:, hlt], g_ds),
                       ("paths", x[:, hlt], x[:, hlt]), ("l1", x[:, l1f], x[:, l1f]), ("paths_l1", x[:, hlt], x[:, l1f])):
        mats = acc.overlap_matrix(name)
        for k in range(3):
            close(mats[k].toarray(), (a.T * W[:, k]) @ b.astype(float))


def test_l1_overlap_flavour():
    run3 = build_maps(["HLT_A", "L1_X", "L1_Y", "L1_FinalOR"], None, SETTINGS)
    assert run3.overlap_l1_flavour() == ""
    ovs = {o.name: o for o in default_overlaps(run3, SETTINGS["overlaps"])}
    assert int(ovs["l1"].rows[1].sum()) == 2 and int(ovs["paths_l1"].cols[1].sum()) == 2   # no L1_FinalOR
    only_bp = build_maps(["HLT_A", "L1_X_beforePrescale"], None, SETTINGS)
    assert only_bp.overlap_l1_flavour() == "beforePrescale"
    edm = build_maps(["HLT_A", "HLT_B"], None, SETTINGS)              # no L1 bits: no L1 matrices
    assert edm.overlap_l1_flavour() is None
    assert {o.name for o in default_overlaps(edm, SETTINGS["overlaps"])} == {"paths"}


# Split datasets: E0-E3 (same paths, round robin by their Dataset_ paths, 2 per stream), Mon (dataset
# prescale 3), TestDataRaw/TestDataScouting (same paths, not split), HLT_X excluded (stays in the dataset
# rates, not in the pure rates).
SPLIT_BITS = ["HLT_A", "HLT_B", "HLT_C", "HLT_X", "Dataset_E0", "Dataset_E1", "Dataset_E2", "Dataset_E3", "Dataset_Mon"]
SPLIT_MENU = {
    "trigger_paths": SPLIT_BITS,
    "datasets": {**{f"E{k}": ["HLT_A", "HLT_B", "HLT_X"] for k in range(4)}, "Mon": ["HLT_A", "HLT_C"],
                 "T": ["HLT_C"], "TestDataRaw": ["HLT_B"], "TestDataScouting": ["HLT_B"]},
    "streams": {"ParkingE0": ["E0", "E1"], "ParkingE1": ["E2", "E3"], "PhysicsT": ["T"], "DQM": ["Mon"],
                "Express": ["Mon", "T"], "PhysicsTest": ["TestDataRaw", "TestDataScouting"]},
    "prescale_columns": ["main"], "prescale_default_column": "main",
    "prescales": {**{f"Dataset_E{k}": [4] for k in range(4)}, "Dataset_Mon": [3]},
}
SPLIT_SETTINGS = dict(SETTINGS, exclude=["^HLT_X$"])


def split_events(rng, n):
    a, b, c, x = (rng.random((n, 4)) < [0.3, 0.2, 0.25, 0.1]).T
    sel_e = a | b | x                         # the Dataset_ paths see all member paths (also excluded ones)
    copy = np.cumsum(sel_e) - 1               # round robin over the selected events
    sel_m = a | c
    return np.column_stack([a, b, c, x, *[sel_e & (copy % 4 == k) for k in range(4)],
                            sel_m & ((np.cumsum(sel_m) - 1) % 3 == 0)])


def dataset_records(bits, menu, x):
    m = build_maps(bits, menu, SPLIT_SETTINGS)
    acc = RateAccumulator(m, default_totals(m, {}), default_overlaps(m, SETTINGS["overlaps"]), block=50)
    acc.add_table(PatternTable.from_bits(bits, x, None))
    sums = SimpleNamespace(datasets=acc.sums["datasets"], streams=acc.sums["streams"],
                           datasets_split=acc.sums["datasets_split"]["fired"],
                           streams_split=acc.sums["streams_split"]["fired"])
    result = SimpleNamespace(maps=m, warnings=[])
    out = _dataset_records(result, sums, 1.0, lambda g: g or "none")
    by_name = [{r["name"]: r for r in recs} for recs in out[:4]]
    return m, acc, (*by_name, out[4], result.warnings)


def test_split_datasets_from_dataset_paths():
    x = split_events(np.random.default_rng(3), 3000)
    a, b, c = x[:, 0], x[:, 1], x[:, 2]
    ab, ac = a | b | x[:, 3], a | c                       # HLT_X (excluded) stays in the dataset rates
    m, acc, (ds, st, ds_split, st_split, check, warnings) = dataset_records(SPLIT_BITS, SPLIT_MENU, x)
    assert m.combined_datasets["E"] == ["E0", "E1", "E2", "E3"]
    assert m.combined_streams["ParkingE"] == ["ParkingE0", "ParkingE1"]
    assert "TestDataRaw" in ds and "TestDataScouting" in ds and m.combined_dataset_streams("E") == ["ParkingE"]
    # combined: OR of the non-excluded paths = sum of the split ones
    assert ds["E"]["count"] == ab.sum() and ds["E"]["written_count"] == ab.sum() and ds["E"]["n_split"] == 4
    assert st["ParkingE"]["count"] == ab.sum() and st["ParkingE"]["written_count"] == ab.sum()
    for k in range(4):
        assert ds_split[f"E{k}"]["method"] == "dataset_path" and ds_split[f"E{k}"]["dataset_prescale"] == 4
        assert ds_split[f"E{k}"]["count"] == (x[:, 4 + k] & ab).sum()
    assert st_split["ParkingE0"]["count"] == ((x[:, 4] | x[:, 5]) & ab).sum()
    assert st_split["ParkingE0"]["method"] == "datasets" and st_split["ParkingE0"]["combined"] == "ParkingE"
    # dataset prescale: the combined dataset fires on its paths, the split one writes 1/3
    mon = x[:, 8] & ac
    assert ds["Mon"]["count"] == ac.sum() and ds_split["Mon"]["count"] == mon.sum() == ds["Mon"]["written_count"]
    assert st_split["Express"]["count"] == (mon | c).sum() and st_split["Express"]["method"] == "datasets"
    assert check["result"] == "ok" and check["checked"] == 2 and not warnings
    # pure rate of E w.r.t. the combined standard+parking datasets (T and TestDataRaw/Scouting; Mon is
    # other), all without the excluded HLT_X
    assert ds["E"]["pure_count"] == (a & ~b & ~c).sum() and ds["E"]["paths_excluded"] == ["HLT_X"]
    # overlaps: between combined datasets/streams
    fake = SimpleNamespace(accumulator=acc, maps=m, normalization=SimpleNamespace(scale=1.0))
    for name, item, other, expected in (("datasets", "E", "T", (ab & c).sum()), ("streams", "ParkingE", "PhysicsT", (ab & c).sum())):
        doc = overlap_document(fake, next(o for o in acc.overlaps if o.name == name), {})
        assert doc["rows"] == list(m.combined_datasets if name == "datasets" else m.combined_streams)
        cells = {(doc["rows"][i], doc["cols"][j]): n for i, j, n, _, _ in doc["entries"]}
        assert cells[(item, item)] == ab.sum() and cells[tuple(sorted((item, other), key=doc["rows"].index))] == expected


def test_split_datasets_estimated_without_dataset_paths():
    x = split_events(np.random.default_rng(4), 3000)
    ab, ac = x[:, 0] | x[:, 1] | x[:, 3], x[:, 0] | x[:, 2]
    # Run-3 NanoAOD: no Dataset_ bits, prescales from the menu
    m, acc, (ds, st, ds_split, st_split, check, warnings) = dataset_records(SPLIT_BITS[:4], SPLIT_MENU, x[:, :4])
    assert ds_split["E0"]["method"] == "paths_div_prescale" and ds_split["E0"]["rate_hz"] == pytest.approx(ab.sum() / 4)
    assert st_split["ParkingE0"]["method"] == "sum_of_datasets"
    assert st_split["ParkingE0"]["rate_hz"] == pytest.approx(ab.sum() / 2)
    assert ds["E"]["written_rate_hz"] == pytest.approx(ab.sum()) and st["ParkingE"]["written_rate_hz"] == pytest.approx(ab.sum())
    assert ds_split["Mon"]["rate_hz"] == pytest.approx(ac.sum() / 3) and ds_split["T"]["method"] == "paths"
    assert ds_split["E0"]["event_size_mb"] == 0.0 and st["ParkingE"]["bandwidth_mb_per_s"] == 0.0
    assert st_split["Express"]["method"] == "upper_bound" and any("upper bound" in w for w in warnings)
    assert check["result"].startswith("not checked")
    # MC menu (no prescale table): split copies / number of copies; their Dataset_ bits are not used
    # (without the PrescaleService every copy accepts all events)
    mc_menu = {k: v for k, v in SPLIT_MENU.items() if not k.startswith("prescale")}
    m, acc, (ds, st, ds_split, st_split, check, warnings) = dataset_records(SPLIT_BITS, mc_menu, x)
    assert ds_split["E0"]["method"] == "paths_div_copies" and ds_split["E0"]["rate_hz"] == pytest.approx(ab.sum() / 4)
    assert ds_split["Mon"]["method"] == "dataset_path"


def test_normalization_formulas():
    ls = normalize.LS_LENGTH_S
    assert math.isclose(ls, 23.3110, abs_tol=1e-4)
    n = normalize.build({"mode": "data", "prescale": 60, "n_datasets": 8, "lumi": 2.2, "target_lumi": 2.0},
                        lumi_sets={"in_file": {(1, i) for i in range(142)}, "with_events": {(1, i) for i in range(140)}})
    assert math.isclose(n.scale, 60 * (2.0 / 2.2) / (142 * ls))
    # MC: genEventCount when unweighted, genEventSumw when weighted; events dropped at production count
    recs = [{"path": f"f{i}", "n_events": 10, "n_read": 10,
             "mc_sums": {"genEventCount": 50.0, "genEventSumw": 75.0}} for i in range(2)]
    n = normalize.build({"mode": "cross_section", "xsec_pb": 100.0, "filter_eff": 0.5, "target_lumi": 7.5},
                        input_records=recs, sumw_processed=20.0, weighted=False)
    assert math.isclose(n.scale, 100 * 0.5 * 7.5 * 0.01 / 100.0) and n.details["fraction_kept_in_ntuples"] == 0.2
    n = normalize.build({"mode": "bunch_crossing", "n_bunches": 2760}, input_records=recs, sumw_processed=20.0,
                        weighted=True)
    assert math.isclose(n.scale, 2760 * 11245.5 / 150.0)
    # no Runs-tree sums: the counted events, with a warning
    n = normalize.build({"mode": "bunch_crossing", "n_bunches": 10}, input_records=[{"path": "f", "n_events": 5,
                        "n_read": 5, "mc_sums": {}}], sumw_processed=5.0, weighted=False)
    assert math.isclose(n.scale, 10 * 11245.5 / 5.0) and n.warnings
    with pytest.raises(normalize.NormalizationError, match="only 1 of 2"):
        normalize.build({"mode": "bunch_crossing", "n_bunches": 10}, input_records=[recs[0], {"path": "g", "n_events": 1,
                        "n_read": 1, "mc_sums": {}}], sumw_processed=5.0, weighted=False)
    with pytest.raises(normalize.NormalizationError, match="not read completely"):
        normalize.build({"mode": "bunch_crossing", "n_bunches": 10}, input_records=[dict(recs[0], n_read=3)],
                        sumw_processed=5.0, weighted=False)
