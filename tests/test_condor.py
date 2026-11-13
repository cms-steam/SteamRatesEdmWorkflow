"""HTCondor executor without a condor pool: submit files (dry run), queue parsing, job states."""

import json
import os

from steamrates import condor
from steamrates.batch import submit
from steamrates.jobstatus import job_states
from steamrates.prepare import prepare


def _prepared(catalog_dir, write_config, tmp_path, batch=""):
    config = write_config("name: m\ninput: {ntuples: DataNano}\njobs: {events_per_job: 100}\n" + batch)
    wd, _ = prepare(config, str(tmp_path / "wd"), catalog_dir=catalog_dir)
    return wd


def test_dry_run_writes_submit_files(catalog_dir, write_config, tmp_path, monkeypatch, capsys):
    monkeypatch.setattr(condor, "submissions", lambda wd: [])
    wd = _prepared(catalog_dir, write_config, tmp_path,
                   f"batch: {{flavour: microcentury, request_memory_mb: 1500, cmssw_base: {tmp_path}}}\n")
    os.makedirs(tmp_path / "src", exist_ok=True)
    submit(wd, local=False, dry_run=True)
    assert "dry run" in capsys.readouterr().out
    subs = [f for f in os.listdir(wd.p("condor")) if f.endswith(".sub")]
    assert len(subs) == 1
    text = open(wd.p("condor", subs[0])).read()
    assert f"initialdir              = {wd.path}" in text
    assert '+JobFlavour             = "microcentury"' in text and "request_memory          = 1500" in text
    assert "transfer_input_files    = code.tar.gz, jobs/$(job_name).json" in text
    assert "partial.root = partials/$(job_name).root" in text
    assert "log                     = logs/condor/cluster_$(ClusterId).log" in text
    assert f'MY.SteamratesWorkdir    = "{wd.path}"' in text
    assert "use_x509userproxy" not in text            # local test files: no proxy
    assert f"$(ClusterId).$(ProcId) {tmp_path}" in text
    queue_file = text.strip().splitlines()[-1].split(" from ")[1]
    assert open(wd.p(queue_file)).read().split("\n")[:4] == ["0 job_0000", "1 job_0001", "2 job_0002", "3 job_0003"]
    assert os.access(wd.p("condor", "run_job.sh"), os.X_OK)
    assert os.path.isdir(wd.p("logs", "condor"))
    assert not os.path.exists(wd.p(condor.SUBMISSIONS))   # nothing was submitted


def test_parse_queue():
    text = ("3 1 100 3 undefined\n"
            "4 2 100 4 undefined\n"
            "5 5 100 5 Job has gone over memory limit of 2000 megabytes.\n"
            "6 4 100 6 undefined\n"            # completed: not in the queue any more
            "3 5 90 1 old held copy\n")         # older cluster: the newer one wins
    q = condor.parse_queue(text)
    assert q == {3: ("queued", "", "100.3"), 4: ("running", "", "100.4"),
                 5: ("held", "Job has gone over memory limit of 2000 megabytes.", "100.5")}


def test_needs_proxy():
    assert not condor.needs_proxy({"proxy": "auto"}, ["/eos/cms/store/a.root", "root://eoscms.cern.ch//eos/cms/a.root"])
    assert condor.needs_proxy({"proxy": "auto"}, ["root://cms-xrd-global.cern.ch//store/a.root"])
    assert condor.needs_proxy({"proxy": True}, ["/eos/a.root"])
    assert not condor.needs_proxy({"proxy": False}, ["root://cms-xrd-global.cern.ch//store/a.root"])


def test_states_with_queue_and_empty_partial(catalog_dir, write_config, tmp_path, monkeypatch):
    wd = _prepared(catalog_dir, write_config, tmp_path)
    jobs = wd.jobs()
    # job 0: failed condor job = empty partial + attempt with exit code 1; job 1: empty partial only
    open(wd.partial(0), "w").close()
    json.dump({"job": 0, "key": jobs[0]["key"], "executor": "condor", "batch_id": "7.0", "host": "b1",
               "exit_code": 1}, open(wd.p("logs", "job_0000.attempt.json"), "w"))
    open(wd.partial(1), "w").close()
    queue = {2: ("running", "", "7.2"), 3: ("held", "memory limit", "7.3")}
    states = {j["id"]: (s, d) for j, s, d in job_states(wd, queue=queue)}
    assert states[0] == ("failed", "exit code 1 (condor 7.0, b1)")
    assert states[1][0] == "failed" and "empty partial" in states[1][1]
    assert states[2] == ("running", "condor job 7.2")
    assert states[3] == ("held", "condor job 7.3: memory limit")

    # submit skips the running job, removes the held one and resubmits it with the failed ones
    removed, submitted = [], []
    monkeypatch.setattr(condor, "submissions", lambda wd: [{"cluster": 7}])
    monkeypatch.setattr(condor, "queue_states", lambda wd: queue)
    monkeypatch.setattr(condor, "remove_jobs", lambda wd, ids: removed.extend(ids))
    monkeypatch.setattr(condor, "submit_jobs",
                        lambda wd, jobs, batch, dry_run=False: submitted.extend(j["id"] for j in jobs)
                        or {"cluster": 8, "schedd_pool": "default", "flavour": "espresso",
                            "request_memory_mb": 2000, "cmssw_base": "x"})
    submit(wd, local=False)
    assert removed == ["7.3"] and submitted == [0, 1, 3]
