"""Sample catalog and measurement config: validation and resolution."""

import pytest

from steamrates.catalog import Catalog
from steamrates.config import load_measurement
from steamrates.lumimask import LumiMask
from steamrates.schema import ConfigError


def test_repository_catalog_loads():
    cat = Catalog.load()
    assert cat.ntuples, "the repository's sample_catalog/ has no ntuples entries"
    for nt in cat.ntuples.values():
        assert nt.raw_id in cat.raw


def test_catalog_ntuples_resolve(catalog_dir, write_config):
    meas = load_measurement(write_config("""
        name: m1
        input: {ntuples: DataNano}
    """), catalog_dir)
    assert meas.raw.id == "DataRaw" and meas.ntuples_in_catalog and meas.raw_in_catalog
    assert meas.counting["format"] == "nano" and meas.counting["process"] is None
    assert meas.target_lumi == 2.0                             # data default: recorded lumi
    assert meas.lumi_mask.n_lumis() == 6
    assert meas.normalization() == {"mode": "data", "prescale": 100.0, "n_datasets": 2, "lumi": 2.0,
                                    "target_lumi": 2.0}
    assert meas.warnings == []


def test_mc_defaults_and_target_lumi(catalog_dir, write_config):
    meas = load_measurement(write_config("""
        name: m1
        input: {ntuples: McNano}
        target_lumi: 5
    """), catalog_dir)
    assert meas.target_lumi == 5.0 and meas.lumi_mask is None


@pytest.mark.parametrize("text, message", [
    ("name: m\ninput: {ntuples: DataNano}\ncountng: {}\n", "did you mean 'counting'"),
    ("name: m\ninput: {ntuples: DataNano}\ncounting: {chunk_sise: 5}\n", "did you mean 'chunk_size'"),
    ("name: m\ninput: {ntuples: DataNano}\ncounting: {chunk_size: -1}\n", "must be > 0"),
    ("name: m\ninput: {ntuples: DataNano}\ncounting: {chunk_size: many}\n", "expected an integer"),
    ("name: m\ninput: {ntuples: DataNano}\njobs: {check_inputs: false}\n", "set files_per_job"),
    ("name: m\ninput: {ntuples: DataNano}\nmerge: {pure_reference: '^HLT_('}\n", "invalid regular expression"),
    ("name: m\ninput: {ntuples: DataNano}\nbatch: {flavour: weekly}\n", "must be one of"),
    ("name: 'my rates'\ninput: {ntuples: DataNano}\n", "not a valid name"),
    ("input: {ntuples: DataNano}\n", "missing key 'name'"),
    ("name: m\ninput: {ntuples: DataNano, produce: {raw: DataRaw}}\n", "exactly one"),
    ("name: m\ninput: {produce: {raw: DataRaw}}\n", "not implemented yet"),
    ("name: m\ninput: {produce: {raw: DataRaw, menu: x}}\n", "unknown key"),
    ("name: m\ninput: {ntuples: NoSuchNano}\n", "no ntuples entry"),
    ("name: m\ninput: {ntuples: DataRaw}\n", "that is a RAW entry"),
    ("name: m\ninput: {ntuples: TodoNano}\n", "TODO"),
    ("name: m\ninput: {ntuples: DataNano}\ncounting: {format: edm}\n", "but the ntuples are 'nano'"),
    ("name: m\ninput: {ntuples: {raw: McRaw, inputs: [a.root], format: edm, weight_branch: w}}\n",
     "no L1 bits and no weight branch"),
    ("name: m\ninput: {ntuples: {raw: MinBiasRaw, inputs: [a.root]}}\ntarget_lumi: 5\n", "bunch_crossing"),
    ("name: m\ninput: {ntuples: {raw: McRaw, inputs: [a.root], lumi_json: g.json}}\n", "MC samples have no lumi"),
    ("name: m\ninput: {ntuples: {raw: {type: mc, normalization: {mode: data, prescale: 1, lumi: 1}},"
     " inputs: [a.root]}}\n", "not possible for type 'mc'"),
])
def test_config_errors(catalog_dir, write_config, text, message):
    with pytest.raises(ConfigError, match=message):
        load_measurement(write_config(text), catalog_dir)


def test_own_ntuples_with_inline_raw_warns(catalog_dir, write_config):
    meas = load_measurement(write_config("""
        name: own
        input:
          ntuples:
            raw: {type: data, normalization: {mode: data, prescale: 10, lumi: 1.5}}
            inputs: [a.root]
            format: nano
    """), catalog_dir)
    assert not meas.raw_in_catalog and meas.catalog_dir is None
    assert any("not taken from the sample catalog" in w for w in meas.warnings)
    assert any("not marked complete" in w for w in meas.warnings)
    assert any("no lumi JSON" in w for w in meas.warnings)
    assert meas.normalization()["n_datasets"] == 1


def test_complete_lumi_json_is_intersected(catalog_dir, write_config, tmp_path):
    (tmp_path / "complete.json").write_text('{"1": [[2, 3], [5, 9]], "2": [[1, 4]]}')
    meas = load_measurement(write_config("""
        name: own
        input:
          ntuples: {raw: DataRaw, inputs: [a.root], format: nano, lumi_json: complete.json}
    """), catalog_dir)
    assert meas.lumi_mask.to_json() == {"1": [[2, 3], [5, 6]]}
    assert not any("not marked complete" in w for w in meas.warnings)


def test_settings_sha1_changes_with_settings(catalog_dir, write_config):
    a = load_measurement(write_config("name: m\ninput: {ntuples: DataNano}\n"), catalog_dir)
    b = load_measurement(write_config("name: m\ninput: {ntuples: DataNano}\ntarget_lumi: 3\n", "b.yaml"), catalog_dir)
    c = load_measurement(write_config("name: m\ninput: {ntuples: DataNano}\n", "c.yaml"), catalog_dir)
    assert a.settings_sha1() != b.settings_sha1()
    assert a.settings_sha1() == c.settings_sha1()


@pytest.mark.parametrize("files, message", [
    ({"a.yaml": "raw:\n  X: {type: data, normalization: {mode: data, prescale: 1, lumi: 1}}\n",
      "b.yaml": "raw:\n  X: {type: data, normalization: {mode: data, prescale: 1, lumi: 1}}\n"}, "already used"),
    ({"a.yaml": "ntuples:\n  N: {raw: Missing, inputs: [x.root], complete: true}\n"}, "unknown RAW entry"),
    ({"a.yaml": "ntuples:\n  N: {raw: X, inputs: [x.root]}\nraw:\n  X: {type: data, normalization: "
                "{mode: data, prescale: 1, lumi: 1}}\n"}, "missing key 'complete'"),
    ({"a.yaml": "raw:\n  X: {type: data, normalization: {mode: data, prescale: 1, lumi: 1, n_bunches: 3}}\n"},
     "unknown key"),
    ({"a.yaml": "samples:\n  X: {}\n"}, "unknown key"),
])
def test_catalog_errors(tmp_path, files, message):
    for name, text in files.items():
        (tmp_path / name).write_text(text)
    with pytest.raises(ConfigError, match=message):
        Catalog.load(str(tmp_path))


def test_todo_is_accepted_in_the_catalog(catalog_dir):
    cat = Catalog.load(catalog_dir)
    assert cat.raw["TodoRaw"].todos() == ["raw TodoRaw.normalization.prescale"]


def test_lumimask_intersection():
    a = LumiMask({1: [(1, 10), (20, 30)], 2: [(1, 5)]})
    b = LumiMask({1: [(5, 25)], 3: [(1, 2)]})
    assert a.intersection(b).to_json() == {"1": [[5, 10], [20, 25]]}
    assert a.intersection(a).to_json() == a.to_json()
    assert a.intersection(b).n_lumis() == 12


def test_example_configs_load():
    import glob
    import os
    examples = glob.glob(os.path.join(os.path.dirname(__file__), "..", "steamrates", "examples", "*.yaml"))
    assert examples
    for path in examples:
        load_measurement(path)
