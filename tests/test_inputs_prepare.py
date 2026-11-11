"""Input expansion, job splitting and `prepare` on synthetic NanoAOD-like files."""

import json
import os

import pytest

from steamrates.inputs import InputError, canonical_name, check_file, expand_inputs, resolve_lfn
from steamrates.prepare import prepare
from steamrates.schema import ConfigError
from steamrates.workdir import WorkDir, split_jobs

# ---- inputs -----------------------------------------------------------------------------------------


def test_canonical_name_matches_eos_and_xrootd():
    a = "/eos/cms/store/data/Run2026D/X/f.root"
    b = "root://eoscms.cern.ch//eos/cms/store/data/Run2026D/X/f.root"
    c = "root://cms-xrd-global.cern.ch//store/data/Run2026D/X/f.root"
    assert canonical_name(a) == canonical_name(b) == canonical_name(c) == "/store/data/Run2026D/X/f.root"


def test_lfn_without_local_copy_goes_to_redirector():
    assert resolve_lfn("/store/user/nobody/none.root") == "root://cms-xrd-global.cern.ch//store/user/nobody/none.root"


def test_expand_glob_list_and_duplicates(tmp_path):
    for name in ("a.root", "b.root", "c.root"):
        (tmp_path / name).write_text("")
    (tmp_path / "list.txt").write_text("# comment\nb.root\n\nc.root  # trailing comment\n")
    res = expand_inputs(["*.root", "list.txt", "a.root"], str(tmp_path))
    assert [os.path.basename(f) for f in res.files] == ["a.root", "b.root", "c.root"]
    assert len(res.duplicates) == 3          # b and c from the list, a given again


@pytest.mark.parametrize("specs, message", [
    (["nothing*.root"], "matches no file"),
    (["missing.root"], "no such file"),
    (["root://eoscms.cern.ch//eos/x/*.root"], "wildcards are not supported"),
    (["outer.txt"], "cannot contain list files"),
    (["empty.txt"], "list file is empty"),
])
def test_expand_errors(tmp_path, specs, message):
    (tmp_path / "inner.txt").write_text("x.root\n")
    (tmp_path / "outer.txt").write_text("inner.txt\n")
    (tmp_path / "empty.txt").write_text("# nothing\n")
    with pytest.raises(InputError, match=message):
        expand_inputs(specs, str(tmp_path))


def test_check_file(catalog_dir, tmp_path):
    ok = check_file(str(tmp_path / "data" / "d1.root"))
    assert (ok.n_events, ok.format, ok.runs, ok.n_lumis, ok.error) == (100, "nano", [1], 2, None)
    (tmp_path / "broken.root").write_text("not a ROOT file")
    assert check_file(str(tmp_path / "broken.root")).error


# ---- job splitting ----------------------------------------------------------------------------------

@pytest.mark.parametrize("n_events, per_job, expected", [
    ([5, 5, 5], 10, [[0, 1], [2]]),
    ([5, 20, 5], 10, [[0], [1], [2]]),           # a large file is a job on its own
    ([5, 0, 5, 0], 10, [[0, 1, 2, 3]]),         # empty files ride along (they carry lumi sections)
    ([3, 3, 3, 3], 6, [[0, 1], [2, 3]]),
    ([], 10, []),
])
def test_split_by_events(n_events, per_job, expected):
    assert split_jobs(n_events, events_per_job=per_job) == expected


def test_split_by_files():
    assert split_jobs([None] * 5, files_per_job=2) == [[0, 1], [2, 3], [4]]
    with pytest.raises(ValueError):
        split_jobs([None, 5], events_per_job=10)


# ---- prepare ----------------------------------------------------------------------------------------

def test_prepare_data(catalog_dir, write_config, tmp_path):
    config = write_config("name: m\ninput: {ntuples: DataNano}\njobs: {events_per_job: 250}\n")
    wd, changed = prepare(config, str(tmp_path / "wd"), catalog_dir=catalog_dir)
    assert changed
    jobs = wd.jobs()
    assert [j["n_events"] for j in jobs] == [200, 200]
    job = json.load(open(wd.job_json(0)))
    assert job["lumi_mask"] == {"1": [[1, 6]]}
    assert job["count_args"][:4] == ["--format", "nano", "--sample", "m"]
    assert "--process" not in job["count_args"]        # the synthetic files have no process in the titles
    resolved = wd.resolved()
    assert resolved["inputs"]["n_events"] == 400 and resolved["settings"]["normalization"]["prescale"] == 100
    assert os.path.exists(os.path.join(wd.code_dir, "steamrates", "count.py")) and os.path.exists(wd.code_tar)
    # same config again: nothing to do
    _, changed = prepare(config, wd.path, catalog_dir=catalog_dir)
    assert not changed


def test_prepare_force_keeps_unchanged_partials(catalog_dir, write_config, tmp_path):
    wd_path = str(tmp_path / "wd")
    prepare(write_config("name: m\ninput: {ntuples: DataNano}\njobs: {events_per_job: 100}\n"), wd_path,
            catalog_dir=catalog_dir)
    wd = WorkDir(wd_path)
    for j in wd.jobs():                                  # pretend all 4 jobs ran
        open(wd.partial(j["id"]), "w").write(str(j["files"]))
    changed = write_config("name: m\ninput: {ntuples: DataNano}\njobs: {events_per_job: 200}\n", "b.yaml")
    with pytest.raises(ConfigError, match="--force"):
        prepare(changed, wd_path, catalog_dir=catalog_dir)
    prepare(changed, wd_path, catalog_dir=catalog_dir, force=True)
    assert [len(j["files"]) for j in wd.jobs()] == [2, 2]
    assert not any(os.path.exists(wd.partial(j["id"])) for j in wd.jobs())    # all jobs changed
    assert len(os.listdir(os.path.dirname(wd.partial(0)))) == 1                # only the stale_* dir
    # only the normalization changes: the partials of all jobs are kept
    for j in wd.jobs():
        open(wd.partial(j["id"]), "w").write(str(j["files"]))
    lumi = write_config("name: m\ninput: {ntuples: DataNano}\njobs: {events_per_job: 200}\ntarget_lumi: 1\n", "c.yaml")
    prepare(lumi, wd_path, catalog_dir=catalog_dir, force=True)
    for j in wd.jobs():
        assert open(wd.partial(j["id"])).read() == str(j["files"])


def test_prepare_rejects_runs_outside_raw(catalog_dir, write_config, tmp_path):
    config = write_config("""
        name: m
        input:
          ntuples: {raw: DataRaw, inputs: [data/*.root], format: nano}
    """)
    golden = os.path.join(catalog_dir, "json", "golden.json")
    json.dump({"7": [[1, 6]]}, open(golden, "w"))          # the RAW input now claims run 7 only
    with pytest.raises(ConfigError, match="runs"):
        prepare(config, str(tmp_path / "wd"), catalog_dir=catalog_dir)


def test_prepare_wrong_process(catalog_dir, write_config, tmp_path):
    config = write_config("name: m\ninput: {ntuples: McNano}\ncounting: {process: HLT2}\n")
    with pytest.raises(ConfigError, match="cannot read the trigger bits"):
        prepare(config, str(tmp_path / "wd"), catalog_dir=catalog_dir)


def test_prepare_unreadable_input(catalog_dir, write_config, tmp_path):
    (tmp_path / "data" / "d9.root").write_text("broken")
    config = write_config("name: m\ninput: {ntuples: DataNano}\n")
    with pytest.raises(ConfigError, match="1 of 5 input files cannot be read"):
        prepare(config, str(tmp_path / "wd"), catalog_dir=catalog_dir)
