"""plots: every figure is written for a small synthetic measurement (smoke test), and the overlap
fractions and partner lists match the merged numbers."""

import os

import numpy as np

from steamrates import plots
from steamrates.merge import merge
from steamrates.results import write_results

from test_merge import run_measurement


def test_plots_smoke(catalog_dir, write_config, tmp_path):
    (tmp_path / "ds.yaml").write_text("DA: [HLT_A]\nDB: [HLT_B]\n")
    (tmp_path / "st.yaml").write_text("PhysicsX: [DA]\nParkingY: [DB]\n")
    config = write_config("name: m\ninput: {ntuples: DataNano}\njobs: {events_per_job: 100}\n"
                          "merge: {datasets: ds.yaml, streams: st.yaml}\n")
    wd = run_measurement(config, str(tmp_path / "wd"), catalog_dir)
    write_results(merge(wd.path), wd.p("results"))
    out = str(tmp_path / "plots")
    written = plots.make_all(wd.p("results"), out, top=5, paths=["HLT_A"], formats=("png",))
    for name in ("rate_budget", "top_hlt_paths", "top_datasets", "top_streams", "overlap_streams",
                 "partners_HLT_A", "matrix_HLT_A"):
        assert name in written and os.path.getsize(os.path.join(out, name + ".png")) > 1000
    assert os.path.exists(os.path.join(out, "index.html"))

    res = plots.Results(wd.p("results"))
    ov = res.overlap("paths")
    a, b = ov.row_index["HLT_A"], ov.row_index["HLT_B"]
    frac = ov.fraction_of_row(["HLT_A", "HLT_B"])
    assert np.isclose(frac[0, 1], ov.count[a, b] / ov.count[a, a]) and np.isclose(frac[1, 0], ov.count[a, b] / ov.count[b, b])
    partners = plots.path_partners(res, "HLT_A", categories=None)
    assert partners[0][0] == "HLT_B" and np.isclose(partners[0][1], ov.rate[a, b])


def test_fmt_rate_err():
    assert plots.fmt_rate_err(9311.63, 40.53) == "9,312 ± 41"
    assert plots.fmt_rate_err(377.16, 8.164) == "377.2 ± 8.2"
    assert plots.fmt_rate_err(6.7, 0.0) == "6.70"
