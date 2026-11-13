# steamrates: rate measurements from trigger ntuples

New implementation of the counting and merging steps (step 2) of the STEAM rate workflow. It lives
next to the legacy scripts in `Rates/` until it has replaced them.

Status (2026-11-12): **counting** and **merging** (normalized rates, JSON + CSV) are implemented and
validated. The sample catalog, measurement configs, `prepare` (work dir + job files), running the
jobs locally (`submit --local`, `run`), `status` and `merge` are implemented. HTCondor submission and
plotting come next.

## Measurements in short

1. Pick an input from the **sample catalog** (`sample_catalog/`, maintained by STEAM). It lists the
   central RAW inputs with everything the normalization needs (prescale, lumi, lumi JSON; MC: cross
   section or number of bunches), and the ntuples made from them (CMSSW, HLT menu, process). The
   schema is documented in `catalog.py`.
   ```bash
   python3 -m steamrates catalog list
   python3 -m steamrates catalog show Run2026D_403663_HLTPhysics_PromptNano
   ```
2. Write a **measurement config** for one input: catalog ntuples, or your own ntuples plus the
   catalog RAW entry they were made from. Examples are in `examples/`; `examples/reference.yaml`
   lists every key with its default.
3. **Prepare** a work dir: checks the config, expands and de-duplicates the input files, opens them
   (events, format, runs), checks the trigger bits of the first file, and writes one job file per
   counting job.
   ```bash
   python3 -m steamrates prepare steamrates/examples/rates_403663_PromptNano.yaml -w /eos/user/x/xyz/rates/403663
   ```
   Running it again does nothing if nothing changed. After changing settings, `--force` updates
   the work dir and keeps the partial results of jobs that did not change.
4. **Run the jobs** and check them. Only jobs without a valid partial result run, so after a
   failure, fix the cause and repeat the command.
   ```bash
   python3 -m steamrates submit <workdir> --local -j 8
   python3 -m steamrates status <workdir>            # done / failed (+ log tail) / invalid / missing
   python3 -m steamrates run <config> -w <workdir> -j 8   # prepare + submit --local + merge
   ```
   Example: the 403663 baseline (3534 EDM files, 13.6M events) takes ~3 min with `-j 8` on lxplus.
5. **Merge** the partial results into normalized rates (`run` does this at the end):
   ```bash
   python3 -m steamrates merge <workdir>                  # -> <workdir>/results/
   python3 -m steamrates merge <workdir> --allow-missing  # use the finished jobs only (flagged incomplete)
   ```
   The merge refuses to run while jobs are missing, if an input file was counted twice, or if a
   combination table does not reproduce the counters of its event loop. Results:
   `summary.json` (input, normalization, totals, checks, warnings), `paths_hlt`, `paths_l1`,
   `datasets`, `streams` (JSON + CSV), `paths_without_dataset.json`, `excluded_paths.json` and the
   overlap matrices `overlaps_*.json`.
   - Split datasets and streams (EGamma0-5, written round robin to stay below the bandwidth of a
     stream) appear twice: `datasets_split`/`streams_split` give what each one writes (from its
     `Dataset_` path, or estimated from the menu's dataset prescale when the ntuples lack that bit);
     `datasets`/`streams` combine them (EGamma = EGamma0 + ... + EGamma5), with pure rates and overlaps.
   - Excluded paths (`merge.exclude`, plus HLT_Physics and HLT_EphemeralPhysics by default) are left
     out of the totals and the pure rates; they keep their own rate and stay in the dataset/stream
     rates and overlaps.
   - The dataset and stream tables have a `bandwidth_mb_per_s` column; event sizes are not known yet,
     so it is 0 for now.

   The formats are documented in `results.py`; the normalization in `normalize.py`; categories
   (standard/parking/scouting/other from the stream names), split datasets and the pure rate
   definitions in `maps.py`. The 403663 baseline merges in ~20 s with < 300 MB.

Each counting job also fills **old-style per-path counters** in the event loop (count, pure count,
HLT/L1 totals; `counters.py`). They cross-check the combination table, and with
`counting: {table: false}` (`count --no-table`) they replace it for very large samples where the
table would be big (MC with nearly one combination per event).

## Idea

A counting job doesn't compute rates. For every event it reads the trigger bits (HLT paths and, if
the ntuple has them, L1 seeds) and stores each **distinct combination of fired bits** once, with the
number of events (MC: sum of weights and sum of squared weights). All rate-like numbers are sums
over these rows and are computed at merge time:
- path, dataset, stream and group rates;
- pure rates;
- overlaps;
- prescaled rates.

Changing a map or a prescale therefore never requires re-reading the ntuples. The docstring of
`patterns.py` has a worked example.

## Usage

Run from the repository root inside a CMSSW environment (`cmsenv`). Only uproot, awkward and
numpy are needed; PyROOT/FWLite are not used.

```bash
# STEAM step-1 ntuples (EDM files with TriggerResults of the re-run HLT)
python3 -m steamrates.count --lumi-json Rates/Json/json_<run>.txt --sample EphemeralHLTPhysics \
    --input-list files_0.txt -o partial_0.root

# NanoAOD data / MC (MC: per-event weights from genWeight)
python3 -m steamrates.count --lumi-json golden.json --sample HLTPhysics -o p.root /eos/cms/store/.../*.root
python3 -m steamrates.count --weight-branch genWeight --sample VBFHToMuMu -o vbf.root --input-list vbf.txt

# Look at / add up partial results (raw counts; no normalization yet)
python3 -m steamrates.summary partial_*.root --pure-reference '^HLT_' --top 30 --csv counts.csv
```

`python3 -m steamrates.count --help` lists all options:
- `--format edm|nano` (auto-detected by default);
- `--process`: the process of the HLT results. Needed when a file has HLT results of several
  processes: EDM files with several TriggerResults (e.g. `MYHLT`), or re-HLT NanoAOD with
  `HLT` and `HLT2` bits;
- `--l1-process` (NanoAOD, for files with L1 bits of several processes);
- `--bits-include` / `--bits-exclude` (NanoAOD branch selection; by default `Flag_*` and the
  always-true cmsDriver step paths such as `L1simulation_step` are excluded);
- `--chunk-size` (default 50000 events; 200k needed ~2x the memory for the same CPU time);
- `--max-events`;
- `--allow-duplicate-events`.

NanoAOD files hold the trigger bits of **every process** that ran paths, and the branch title names
the process (`Trigger/flag bit (process: HLT2)`). When two processes have a path of the same name,
one of the branches gets a `_p<process>` suffix. The reader takes the HLT-path bits of one process
and the L1 bits of one process (in Run-3 PromptReco: `HLT` and `RECO`) and removes the suffix. If
there is a choice to make and no option is given, it stops with a list of the processes. The chosen
processes are stored in the partial's metadata (`bit_processes`).

Safety features:
- **Duplicate input files are refused**, also when the same file appears under two names (for
  example `/eos/cms/store/...` and `root://.../store/...`).
- **Repeated events are an error by default.** A repeated (run, lumi, event) usually means
  overlapping inputs.
- **The output is written atomically.** It goes to a temporary file next to `-o` and is then
  renamed, and nothing else is written. Jobs of different measurements can run in parallel in the
  same directory.

## Package layout

| module | content |
|---|---|
| `__main__.py` | `python3 -m steamrates <command>`: run, prepare, submit, status, merge, catalog, count, summary |
| `catalog.py` | sample catalog: RAW inputs and ntuples (schema in the docstring) |
| `config.py` | measurement config: validation, resolution against the catalog |
| `prepare.py` | command line: config → work dir with job files |
| `workdir.py` | work-dir layout, job splitting |
| `inputs.py` | input specs (globs, list files, LFNs), duplicate removal, per-file checks, `open_root` (xrootd) |
| `schema.py` | YAML validation helpers (unknown keys, types, `TODO` placeholders) |
| `provenance.py` | git state of the code and catalog |
| `batch.py` | command line `submit`: local executor (HTCondor later) |
| `jobstatus.py` | command line `status`: done / failed / invalid / missing |
| `jobrun.py` | runs one job of a work dir (used by the executors) |
| `run.py` | command line `run`: prepare + submit --local |
| `counters.py` | old-style per-bit counters (cross-check of the table; fallback with `--no-table`) |
| `count.py` | command line: input files → one partial result |
| `summary.py` | command line: inspect / add up partial results |
| `merge.py` | command line `merge`: partial results -> normalized rates (checks, two passes over the partials) |
| `normalize.py` | data / MC cross-section / MC bunch-crossing normalization, constants (f_rev, LS length) |
| `maps.py` | bit kinds, datasets/streams, categories from stream names, pure-rate reference groups |
| `rates.py` | streaming accumulator: per-path/dataset/stream sums, pure and shared sums, totals, overlaps |
| `results.py` | output files (JSON, CSV) and the printed summary |
| `readers/edm.py` | `edm::TriggerResults` + event IDs decoded from raw bytes (byte layouts documented in the module) |
| `readers/nano.py` | NanoAOD flat `HLT_*`/`L1_*`/... bool branches, `genWeight`, LuminosityBlocks/Runs trees |
| `readers/base.py` | reader interface (`EventChunk`, `FileInfo`) |
| `rootbytes.py` | helpers to decode ROOT-serialized bytes |
| `menu.py` | HLT menu from the file's `ParameterSets` tree: path names, datasets, streams, prescales |
| `lumimask.py` | vectorized lumi-JSON selection |
| `patterns.py` | `PatternTable`: the combination table (build, add, remap menus, CSR form in blocks, per-bit totals, pure counts) |
| `partial.py` | partial-result file format (read/write) and `merge_partials` |

## Partial-result file

The file is written by uproot with zstd compression. It contains three objects.

**`metadata`**: a TObjString holding a JSON document with:
- the bit names;
- the sample label;
- the menu (name, datasets, streams, prescale table);
- the lumi mask;
- the inputs, with event counts per file;
- totals and MC sums (`genEventSumw`, ...);
- the command line, host, version and runtime.

**`patterns`**: a TTree with one entry per distinct combination, with branches:
- `fired`: the indices of the fired bits;
- `count`, `sumw`, `sumw2`.

**`lumis`**: a TTree with one entry per (run, lumi section), with branches:
- `n_events`: the number of selected events;
- `in_file`: whether the lumi section is listed in the input's LuminosityBlocks tree.

Lumi sections are counted per `--sample` at merge time, each (run, lumi) once, however many jobs it
was spread over.

## Validation (2026-11-09)

- **EDM reader vs FWLite, event by event:** accept bits, path names and run/lumi/event agree. Checked
  on GRunV51/V59/V62 and two HI menus (1001 and 394 paths).
- **Run 402360, GRunV59, all 1136 files:** compared with the legacy `nomaps` outputs in
  `Rates/402360_HLTV1p2`. Events (5,721,135) and the total and pure counts of all 999 paths are
  identical. Lumi sections: 142 unique (the legacy N_LS = 1136 = 8 EphemeralHLTPhysics datasets
  × 142). Runtime: 63 s with 8 parallel jobs.
- **Run 403663 HLTPhysics NanoAOD, lumi sections 400–1700:** 556,746 events and 1301 lumi
  sections; `HLT_IsoMu24` has 2138 counts. These match the earlier independent check.
- **VBF H→μμ NANOAODSIM with `genWeight`:** the summed weights equal `genEventSumw`, and the
  per-path weighted sums match a direct numpy calculation.
- **Phase-2 TTbar re-HLT ntuples** (first version of the common format, 2026-11-10; 90 files,
  9000 events, HLT bits of the processes `HLT` and `HLT2`): with `--process HLT2` and with
  `--process HLT`, the total and pure counts of all bits (34 and 31) match a direct numpy
  calculation. After the change to multi-process selection, Run 403663 NanoAOD and 402360 EDM give
  the same counts as before. The only difference is that the always-true `L1Reco_step` is no longer
  in the table.
