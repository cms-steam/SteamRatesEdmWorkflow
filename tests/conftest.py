"""Shared fixtures: a small sample catalog and synthetic NanoAOD-like files (no EOS needed).

Run from the repository root inside cmsenv:  python3 -m pytest tests
"""

import json
import textwrap

import numpy as np
import pytest
import uproot

CATALOG = """
raw:
  DataRaw:
    type: data
    lumi_json: json/golden.json
    normalization: {mode: data, prescale: 100, n_datasets: 2, lumi: 2.0}
  McRaw:
    type: mc
    normalization: {mode: cross_section, xsec_pb: 50.0, filter_eff: 0.5, target_lumi: 7.5}
  MinBiasRaw:
    type: mc
    normalization: {mode: bunch_crossing, n_bunches: 2760}
  TodoRaw:
    type: data
    normalization: {mode: data, prescale: TODO, lumi: 2.0}
ntuples:
  DataNano:
    raw: DataRaw
    format: nano
    inputs: ["{data_glob}"]
    complete: true
  McNano:
    raw: McRaw
    format: nano
    inputs: [mc_files.txt]
    complete: true
  TodoNano:
    raw: TodoRaw
    inputs: [x.root]
    complete: true
"""


def write_nano(path, run, lumis, n_per_lumi, rng, bits=("HLT_A", "HLT_B", "L1_X")):
    """A NanoAOD-like file: Events (run, luminosityBlock, event, bool trigger bits) and
    LuminosityBlocks (run, luminosityBlock)."""
    lumi = np.repeat(np.asarray(lumis, np.uint32), n_per_lumi)
    n = len(lumi)
    events = {"run": np.full(n, run, np.uint32), "luminosityBlock": lumi,
              "event": np.arange(n, dtype=np.uint64) + 1000 * int(lumis[0])}
    for b in bits:
        events[b] = rng.random(n) < 0.3
    lumi_blocks = {"run": np.full(len(lumis), run, np.uint32), "luminosityBlock": np.asarray(lumis, np.uint32)}
    with uproot.recreate(path) as f:
        f.mktree("Events", {k: v.dtype for k, v in events.items()})
        f["Events"].extend(events)
        f.mktree("LuminosityBlocks", {k: v.dtype for k, v in lumi_blocks.items()})
        f["LuminosityBlocks"].extend(lumi_blocks)


@pytest.fixture
def catalog_dir(tmp_path):
    """Catalog + 4 data files (2 lumi sections each) + 2 MC files, all in tmp_path."""
    rng = np.random.default_rng(1)
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    for i in range(4):
        write_nano(str(data_dir / f"d{i}.root"), 1, [2 * i + 1, 2 * i + 2], 50, rng)
    mc_dir = tmp_path / "mc"
    mc_dir.mkdir()
    for i in range(2):
        write_nano(str(mc_dir / f"m{i}.root"), 1, [i + 1], 30, rng)
    cat = tmp_path / "catalog"
    (cat / "json").mkdir(parents=True)
    (cat / "json" / "golden.json").write_text(json.dumps({"1": [[1, 6]]}))
    (cat / "mc_files.txt").write_text(f"# MC files\n{mc_dir}/m0.root\n{mc_dir}/m1.root  # second\n")
    (cat / "samples.yaml").write_text(CATALOG.replace("{data_glob}", str(data_dir / "*.root")))
    return str(cat)


@pytest.fixture
def write_config(tmp_path):
    def write(text, name="meas.yaml"):
        path = tmp_path / name
        path.write_text(textwrap.dedent(text))
        return str(path)
    return write
