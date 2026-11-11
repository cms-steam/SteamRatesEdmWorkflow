# steamrates: counting step

New implementation of the counting step (step 2) of the STEAM rate workflow. It lives next to the
legacy scripts in `Rates/` until it has replaced them.

Status (2026-11-11): **counting** is implemented and validated. The sample catalog, measurement
configs and `prepare` (work dir + job files) are implemented. Running the jobs (locally and on
HTCondor), merging into normalized rates (JSON output for the website) and plotting come next.

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
| `__main__.py` | `python3 -m steamrates <command>`: prepare, catalog, count, summary |
| `catalog.py` | sample catalog: RAW inputs and ntuples (schema in the docstring) |
| `config.py` | measurement config: validation, resolution against the catalog |
| `prepare.py` | command line: config → work dir with job files |
| `workdir.py` | work-dir layout, job splitting |
| `inputs.py` | input specs (globs, list files, LFNs), duplicate removal, per-file checks, `open_root` (xrootd) |
| `schema.py` | YAML validation helpers (unknown keys, types, `TODO` placeholders) |
| `provenance.py` | git state of the code and catalog |
| `count.py` | command line: input files → one partial result |
| `summary.py` | command line: inspect / add up partial results |
| `readers/edm.py` | `edm::TriggerResults` + event IDs decoded from raw bytes (byte layouts documented in the module) |
| `readers/nano.py` | NanoAOD flat `HLT_*`/`L1_*`/... bool branches, `genWeight`, LuminosityBlocks/Runs trees |
| `readers/base.py` | reader interface (`EventChunk`, `FileInfo`) |
| `rootbytes.py` | helpers to decode ROOT-serialized bytes |
| `menu.py` | HLT menu from the file's `ParameterSets` tree: path names, datasets, streams, prescales |
| `lumimask.py` | vectorized lumi-JSON selection |
| `patterns.py` | `PatternTable`: the combination table (build, add, remap menus, CSR form, per-bit totals, pure counts) |
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
