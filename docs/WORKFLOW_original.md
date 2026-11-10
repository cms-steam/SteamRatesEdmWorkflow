# SteamRatesEdmWorkflow – how the current workflow fits together

This page describes the workflow **as it is today** (the state before the planned refactor).
It is meant as a map for people who need to run the workflow, debug it, or change it.

The diagrams are written in [Mermaid](https://mermaid.js.org/). They render directly on GitHub.
In VS Code you need a Mermaid-capable Markdown preview, for example the
"Markdown Preview Mermaid Support" extension.

**Colour key, the same in all diagrams:**

| Colour | Meaning |
|---|---|
| 🟨 yellow | file or values **you edit by hand** before a run |
| 🟦 blue | script that is run (by you or by another script) |
| ⬜ grey | generated file or directory |
| 🟪 purple | HTCondor batch step |
| 🟧 orange | works, but has a known problem (see [Known issues](#known-issues)) |
| 🟥 red, dashed | broken or legacy |

Contents:
1. [Big picture](#1-big-picture)
2. [Step 1 – producing the HLT ntuples (`Prod/`)](#2-step-1--producing-the-hlt-ntuples-prod)
3. [Step 2a – counting jobs (`Rates/`)](#3-step-2a--counting-jobs-rates)
4. [What the counting script does per event](#4-what-the-counting-script-does-per-event)
5. [Step 2b – merging and computing rates](#5-step-2b--merging-and-computing-rates)
6. [The trigger maps (`STEAM_maps/` → `Menu_HLT.py`)](#6-the-trigger-maps-steam_maps--menu_hltpy)
7. [Data vs. MC](#7-data-vs-mc)
8. [Other tools in the repo](#8-other-tools-in-the-repo)
9. [Where every user setting lives](#9-where-every-user-setting-lives)
10. [Known issues](#known-issues)
11. [Design principles for the refactor](#design-principles-for-the-refactor)

---

## 1. Big picture

The workflow re-runs a new HLT menu on recorded RAW data. It then counts how often each trigger path fires and converts the counts into rates at a chosen target luminosity.

```mermaid
flowchart LR
    RAW[("CMS RAW data<br/>e.g. EphemeralHLTPhysics0-7, one run")]

    subgraph S1 ["Step 1 · Prod/ · re-run the HLT"]
        P1["hltGetConfiguration<br/>+ run_steamflow_cfg.py"]
        P2{{"HTCondor<br/>cmsRun, one job per RAW file"}}
        P1 --> P2
    end

    NT[("hlt_N.root ntuples<br/>contain ONLY edm::TriggerResults::MYHLT")]

    subgraph S2 ["Step 2a · Rates/ · count"]
        C1{{"HTCondor<br/>triggerCountsFromTriggerResults.py<br/>one job per ntuple"}}
        C2[("Results/Data/Raw/*/<br/>per-job count CSVs")]
        C1 --> C2
    end

    subgraph S3 ["Step 2b · Rates/ · merge and scale"]
        M1["mergeOutputs.py"]
        OUT[("Results/Data/output.*.csv<br/>counts and rates in Hz")]
        M1 --> OUT
    end

    JSON["Json/json_RUN.txt<br/>selected lumi sections"]
    MAPS["Menu_HLT.py<br/>path → dataset / stream / group"]
    NORM["lumi_in, lumi_target, hlt_ps"]

    RAW --> P1
    P2 --> NT --> C1
    C2 --> M1
    JSON -.-> C1
    MAPS -.->|"only if maps != nomaps"| C1
    MAPS -.-> M1
    NORM -.-> M1

    classDef cfg fill:#fff3bf,stroke:#e0a800,color:#000
    classDef script fill:#d0ebff,stroke:#1c7ed6,color:#000
    classDef data fill:#e9ecef,stroke:#868e96,color:#000
    classDef condor fill:#e5dbff,stroke:#7048e8,color:#000
    class JSON,NORM cfg
    class P1,M1 script
    class RAW,NT,C2,OUT,MAPS data
    class P2,C1 condor
```

Step 1 is likely to be replaced by a common L1+HLT ntuple format produced in another repository. Step 2 is the focus of the modernization.

---

## 2. Step 1 – producing the HLT ntuples (`Prod/`)

```mermaid
flowchart TD
    CONFDB[("HLT menu in ConfDB<br/>e.g. /dev/CMSSW_16_0_0/GRun")]
    HGC["hltGetConfiguration ... --process MYHLT --prescale ... > hlt.py"]
    HLTPY[("hlt.py")]
    SEED["L1SeedRenamer*.py / L1SeedDisabler.py / L1Workaround.py<br/>optional: rewrite L1 seed names in hlt.py in place"]
    DUMP["edmConfigDump hlt.py > hlt_config.py"]
    HCFG[("hlt_config.py")]

    DAS[("DAS")]
    CFL["createFileList.py<br/>one dasgoclient query PER LUMI SECTION<br/>⚠ produces duplicate files"]
    CFLL["createFileList_Local.py (glob on EOS)<br/>createHIonFilelist.sh (heavy ions)"]
    LCFF[("list_cff_*.py<br/>inputFileNames = [...]")]

    RSC["run_steamflow_cfg.py<br/>EDIT: which list_cff to import, nEvents,<br/>switchL1PS / columnL1PS"]
    LOCAL["local test: cmsRun run_steamflow_cfg.py<br/>with nEvents = 500"]
    COND["cmsCondorData.py run_steamflow_cfg.py CMSSW_src OUTDIR<br/>-n files/job -q flavour -p proxy"]
    JOBS[("Jobs/run_cfg.py (cfg dump + argparse for input files)<br/>Jobs/Job_i/sub_i.sh<br/>condor_cluster.sub, sub_total.jobb")]
    SUB{{"./sub_total.jobb → condor_submit"}}
    RUN{{"each job, in TMPDIR:<br/>cmsRun run_cfg.py --inputFiles=...<br/>1 thread, output keeps only TriggerResults::MYHLT"}}
    OUTNT[("OUTDIR/hlt_i.root")]
    CRASH["check_crashes.sh<br/>grep job stderr for crashes"]

    CONFDB --> HGC --> HLTPY --> DUMP --> HCFG
    SEED -.-> HLTPY
    DAS --> CFL --> LCFF
    CFLL --> LCFF
    HCFG --> RSC
    LCFF --> RSC
    RSC --> LOCAL
    RSC --> COND --> JOBS --> SUB --> RUN --> OUTNT
    RUN -.-> CRASH

    classDef cfg fill:#fff3bf,stroke:#e0a800,color:#000
    classDef script fill:#d0ebff,stroke:#1c7ed6,color:#000
    classDef data fill:#e9ecef,stroke:#868e96,color:#000
    classDef condor fill:#e5dbff,stroke:#7048e8,color:#000
    classDef warn fill:#ffe8cc,stroke:#f76707,color:#000
    class RSC cfg
    class HGC,SEED,DUMP,CFLL,LOCAL,COND,CRASH script
    class CONFDB,HLTPY,HCFG,DAS,LCFF,JOBS,OUTNT data
    class SUB,RUN condor
    class CFL warn
```

- `run_steamflow_cfg.py` does `from hlt_config import *`, then replaces the source, adds an output module that keeps only `edmTriggerResults_*_*_MYHLT`, and forces 1 thread.
- Optionally it inserts an `L1TGlobalPrescaler` to emulate a tighter L1 prescale column.
- The ntuples therefore contain **only HLT path decisions**. There is no L1 information, no event content, and no per-module information beyond what `TriggerResults` stores.
- `Prod/run_alcaval.py` + `Prod/config.json` is a separate AlCa validation flow (reference vs. target global tag). It is not part of the rate workflow.

---

## 3. Step 2a – counting jobs (`Rates/`)

```mermaid
flowchart TD
    CFG["config_makeCondorJobsData.py<br/>EDIT: inputFilesDir, cmsswDir, json_file,<br/>file_type, maps, n per job, flavour"]
    CSR["condorScriptForRatesData.py"]
    WIPE["rm -rf Jobs"]
    MRF["make_ratesFilesInputData.py<br/>recursive ls of inputFilesDir"]
    FID[("filesInputData.py<br/>⚠ overwritten inside the source tree<br/>(old one copied to filesInputData_old.py)")]
    J[("Jobs/sub_raw/sub_i.sh — one per ntuple<br/>Jobs/Job_i/sub_i.sh — groups of n<br/>condor_cluster.sub, sub_total.jobb")]
    SUB{{"./sub_total.jobb<br/>first: rm Results/Data/Raw/*/*.csv and *.root<br/>then: condor_submit"}}

    subgraph JOB ["each condor job · runs inside the shared working directory"]
        TC["triggerCountsFromTriggerResults.py<br/>-i hlt_i.root -j json -s i -f custom -m maps"]
        JOUT[("Jobs/output.global.i.csv<br/>Jobs/output.path.misc.i.csv<br/>(+ physics/scouting/parking/dataset/group/stream/corr CSVs with maps)<br/>Jobs/histos.i.root")]
        HFT["handleFileTransfer.py -d WORKDIR -s i<br/>mkdir + mv into Results/"]
        TC --> JOUT --> HFT
    end

    RAWRES[("Results/Data/Raw/<br/>Global/ PathMisc/ PathPhysics/ PathScouting/ PathParking/<br/>DatasetPhysics/ Group/ Stream/ *Corr/ Root/ ...")]

    JSON["Json/json_RUN.txt"]
    MENU[("Menu_HLT.py")]
    TSE["triggerstoexclude.py<br/>paths ignored for pure rates"]
    AUX["aux.py<br/>stream classification, mergeNames"]

    CFG -->|os.system| CSR
    CSR --> WIPE
    CSR -->|os.system| MRF --> FID --> CSR
    CSR --> J --> SUB --> TC
    HFT --> RAWRES
    JSON -.-> TC
    MENU -.->|maps only| TC
    TSE -.-> TC
    AUX -.-> TC

    classDef cfg fill:#fff3bf,stroke:#e0a800,color:#000
    classDef script fill:#d0ebff,stroke:#1c7ed6,color:#000
    classDef data fill:#e9ecef,stroke:#868e96,color:#000
    classDef condor fill:#e5dbff,stroke:#7048e8,color:#000
    classDef warn fill:#ffe8cc,stroke:#f76707,color:#000
    class CFG,JSON,TSE cfg
    class CSR,MRF,TC,HFT,AUX script
    class J,JOUT,RAWRES,MENU data
    class SUB condor
    class WIPE,FID warn
```

- Everything is wired together with `os.system(...)` calls and fixed relative paths: `Jobs/`, `Results/`, `filesInputData.py`, `sub_total.jobb`, `condor_cluster.sub`.
- Starting a new measurement **deletes the previous one's job directory and raw results**. Two measurements can therefore not run from the same checkout at the same time.
- `file_type`: `custom` reads `TriggerResults::MYHLT` (STEAM ntuples). `RAW` reads `TriggerResults::HLT` (online decisions in RAW). `L1Accept` is like `RAW`, but keeps only events where `DST_Physics` fired.

---

## 4. What the counting script does per event

`Rates/triggerCountsFromTriggerResults.py` processes **one ntuple file**:

```mermaid
flowchart TD
    START(["open file with FWLite Events"])
    FIRST{"first event?"}
    INIT["read path names from TriggerResults<br/>skip HLTriggerFirstPath / HLTriggerFinalPath<br/>maps: look up dataset, group, stream, type of each path"]
    JS{"(run, LS) inside lumi JSON?<br/>⚠ JSON file re-read for every event"}
    LSC["remember (run, LS) → N_LS of this job"]
    L1A{"file_type = L1Accept<br/>and DST_Physics did not fire?"}
    LOOP["loop over all paths: accept(index)?"]
    NOMAP["nomaps: count how many HLT_* paths fired"]
    MAP["maps: per fired path → dataset / group / stream counters,<br/>physics / scouting / parking / misc event counters<br/>⚠ slow linear scans through Menu_HLT maps"]
    TOT["total count += 1 for each fired path"]
    PURE{"exactly one counted path fired,<br/>it is HLT_*, and not in triggerstoexclude?"}
    PC["pure count += 1 for that path"]
    CORR["maps only: trigger × dataset and dataset × dataset<br/>correlation matrices, group pure / shared counts"]
    NEXT(["next event"])
    WRITE[("after the loop:<br/>output.global.i.csv (N_LS, N_events)<br/>output.path.*.i.csv (path, total count, pure count)<br/>maps: output.dataset.* / group / stream / *_corr CSVs<br/>histos.i.root")]

    START --> FIRST
    FIRST -->|yes| INIT --> JS
    FIRST -->|no| JS
    JS -->|no| NEXT
    JS -->|yes| LSC --> L1A
    L1A -->|yes| NEXT
    L1A -->|no| LOOP
    LOOP --> NOMAP --> TOT
    LOOP --> MAP --> TOT
    TOT --> PURE
    PURE -->|yes| PC --> CORR
    PURE -->|no| CORR
    CORR --> NEXT
    NEXT --> FIRST
    NEXT -->|end of file| WRITE

    classDef script fill:#d0ebff,stroke:#1c7ed6,color:#000
    classDef data fill:#e9ecef,stroke:#868e96,color:#000
    classDef warn fill:#ffe8cc,stroke:#f76707,color:#000
    class START,INIT,LSC,LOOP,NOMAP,TOT,PC,CORR,NEXT script
    class WRITE data
    class JS,MAP warn
```

Definitions used by the current code:
- **Total count of a path**: the number of selected events in which the path fired.
- **Pure count of a path**: the number of events in which this path was the *only* "counted" path that fired.
  - With `nomaps`, "counted" means any `HLT_*` path.
  - With maps, it means any path in a physics stream (excluding HLTPhysics, ZeroBias, parking and the scouting monitors).
  - Paths in `triggerstoexclude.py` never get a pure count.
- The CSV columns are named "Rate (Hz)", but at this stage they hold **raw counts**. They are converted to rates in the merge step.

---

## 5. Step 2b – merging and computing rates

```mermaid
flowchart TD
    CFGM["config_mergeOutputsData.py<br/>EDIT: lumi_in, lumi_target, hlt_ps,<br/>maps, makeFigures, diffLoc / files_dir"]
    RAWRES[("Results/Data/Raw/*")]
    LIST["aux.makeListsOfRawOutputs<br/>unreadable CSV / zombie ROOT → bad job, excluded"]
    MO["mergeOutputs.py -l lumi_in -t lumi_target -p hlt_ps -m maps"]
    SUM["sum N_LS, N_events and all counts over jobs"]
    SF["scale = lumi_target / lumi_in × hlt_ps / (N_LS × 23.31 s)<br/>rate = count × scale"]
    SORT["sort paths by count (datasets: by stream, then rate)"]
    OUT[("Results/Data/output.global.csv<br/>Results/Data/output.path.misc.csv (nomaps)<br/>+ path.physics/scouting/parking, dataset.*, group, stream (maps)")]
    HADD["-f: hadd histos.*.root → Results/Data/histos.root<br/>re-sort and scale correlation TH2s"]
    DRAW["Draw.py → Figures/<br/>dataset-dataset / trigger-dataset overlaps, group pie charts"]

    CFGM -->|os.system| MO
    RAWRES --> LIST --> MO
    MO --> SUM --> SF --> SORT --> OUT
    MO -->|"makeFigures and maps != nomaps"| HADD
    CFGM -->|os.system| DRAW
    HADD --> DRAW

    classDef cfg fill:#fff3bf,stroke:#e0a800,color:#000
    classDef script fill:#d0ebff,stroke:#1c7ed6,color:#000
    classDef data fill:#e9ecef,stroke:#868e96,color:#000
    class CFGM cfg
    class LIST,MO,SUM,SF,SORT,HADD,DRAW script
    class RAWRES,OUT data
```

**Scale factor, worked example** (run 402360, 8 EphemeralHLTPhysics datasets, LS 193–334):

| quantity | value |
|---|---|
| N_LS (summed over jobs) | 1136 = 8 datasets × 142 LS |
| N_events | 5,721,135 |
| hlt_ps | 480 = 60 (HLTPhysics prescale) × 8 (number of Ephemeral datasets) |
| lumi_in = lumi_target | 2.2e34 |
| scale | 1 × 480 / (1136 × 23.31 s) = 0.01813 Hz per count |

Note that the factor 8 appears in **both** N_LS and hlt_ps and cancels. This is only correct as long as every (dataset, LS) pair ends up in exactly one ntuple file (see [Known issues](#known-issues)).

---

## 6. The trigger maps (`STEAM_maps/` → `Menu_HLT.py`)

The maps assign each path to its stream, primary dataset, physics group(s) and type. They are only needed with `maps = somemaps / allmaps`, and for the figures.

```mermaid
flowchart LR
    MENU[("HLT menu in ConfDB")]
    MK["make_MenuHLT.py MENU_NAME"]
    A["hltConfigFromDB → hlt.py"]
    B["hltDumpStream --csv --clean hlt.py<br/>(needs cx_Oracle)"]
    C[("outputfile.csv")]
    D["csv_to_tsv.py"]
    E[("steamdb.tsv")]
    F["makeMaps.py"]
    G[("STEAM_maps/Menu_HLT.py")]
    H[("Rates/Menu_HLT.py<br/>datasetStreamMap, streamMap, datasetMap,<br/>groupMap, typeMap, (status/target/flatness/enable)")]
    USERS["imported by:<br/>triggerCountsFromTriggerResults.py, aux.py,<br/>mergeOutputs.py, Draw.py"]
    NDM["newDatasetMap — needed by maps = allmaps<br/>does not exist in any Menu_HLT.py"]

    MENU --> MK --> A --> B --> C --> D --> E --> F --> G
    G -.->|"copied by hand"| H --> USERS
    NDM -.-> USERS

    classDef script fill:#d0ebff,stroke:#1c7ed6,color:#000
    classDef data fill:#e9ecef,stroke:#868e96,color:#000
    classDef broken fill:#ffe3e3,stroke:#e03131,stroke-dasharray: 5 5,color:#000
    class MK,A,B,D,F,USERS script
    class MENU,C,E,G,H data
    class NDM broken
```

Map keys are path names with the version number stripped (`HLT_IsoMu24_v`). The stripping is done with `rstrip("0123456789")`, which also strips digits that are part of the name (`Dataset_AlCaP0` → `Dataset_AlCaP`).

---

## 7. Data vs. MC

The MC chain is a near-copy of the data chain. The real differences are only (1) how the input files are listed and selected, and (2) the normalization.

Each row below is one stage. The left box is the Data script and the right box its MC twin (dotted line = near-copy).

```mermaid
flowchart TD
    subgraph P ["Step 1 · create ntuple production jobs"]
        direction LR
        d1["DATA · Prod/cmsCondorData.py<br/>files from list_cff_*.py"]
        m1["MC · Prod/cmsCondorMC.py<br/>files from DAS for each dataset in map_MCdatasets_xs.py<br/>(+ one hardcoded dataset)"]
        d1 -.-|"≈ copy"| m1
    end

    subgraph C ["Step 2a · create counting jobs"]
        direction LR
        d2["DATA · config_makeCondorJobsData.py<br/>→ condorScriptForRatesData.py<br/>→ make_ratesFilesInputData.py (flat file list)"]
        m2["MC · config_makeCondorJobsMC.py<br/>→ condorScriptForRatesMC.py<br/>→ make_ratesFilesInputMC.py (dataset → files)"]
        d2 -.-|"≈ copy"| m2
    end

    subgraph S ["Step 2a · run counting jobs — SHARED code"]
        SHARED["triggerCountsFromTriggerResults.py<br/>(data: -j lumi JSON · MC: -j DATASET_NAME, detected because it is not a .txt/.json file)<br/>handleFileTransfer.py, aux.py, Menu_HLT.py"]
    end

    subgraph M ["Step 2b · merge and normalize"]
        direction LR
        d5["DATA · config_mergeOutputsData.py → mergeOutputs.py<br/>scale = lumi_t / lumi_in × hlt_ps / (N_LS × 23.31 s)"]
        m5["MC · config_mergeOutputsMC.py → prepareMergeOutputsMC.py<br/>→ mergeOutputs.py -w DATASET, once per dataset<br/>scale = lumi_t × σ[pb] × 0.01 / N_events"]
        XS[("MCDatasets/map_MCdatasets_xs.py<br/>dataset → σ (currently only 2018 RelVals)")]
        d5 -.-|"≈ copy"| m5
        XS -.-> m5
    end

    P --> C --> S --> M

    classDef cfg fill:#fff3bf,stroke:#e0a800,color:#000
    classDef script fill:#d0ebff,stroke:#1c7ed6,color:#000
    classDef data fill:#e9ecef,stroke:#868e96,color:#000
    class XS cfg
    class d1,d2,d5,m1,m2,m5,SHARED script
```

MC will become the main use case (HL-LHC / Phase-2 menu preparation), so the refactored code should treat data and MC as two configurations of **one** pipeline (see [Design principles](#design-principles-for-the-refactor)).

---

## 8. Other tools in the repo

```mermaid
flowchart TD
    subgraph CMP ["Comparing two HLT configurations on the same input"]
        HD["Rates/serial_hltDiff.py -o OLD_DIR -n NEW_DIR<br/>runs hltDiff on matching hlt_*.root → merged_hltDiff.csv"]
        CD["Rates/checkDiff.sh<br/>hardcoded AlCa dirs; drops tiny files, runs hltDiff"]
        AV["Prod/run_alcaval.py + Prod/config.json<br/>AlCa validation: ref vs. target global tag"]
    end

    subgraph OVL ["Single-path overlap study"]
        OV["Rates/check_overlaps.py<br/>everything hardcoded (path, lumi, json, dir)<br/>→ overlap_PATH.csv"]
    end

    subgraph WBM ["Comparison with online rates (WBM)"]
        W1["config_WBMcomparison.py"]
        W2["makeWBMCSV.py + cernSSOWebParser2.py + htmlTableParser.py<br/>SSO cookie scraping"]
        W3["compareWBM.py<br/>imports psL1TMap / psHLTMap that do not exist"]
        W1 --> W2 --> W3
    end

    subgraph LEG ["Legacy / leftovers"]
        L1["STEAM_maps/adjustPS.py (imports missing outputMaps)"]
        L2["Rates/make_files2.py (Run 2017 Tier-0 lists)"]
        L3["Rates/createFileList.py (stale copy of Prod/createFileList.py)"]
        L4["Rates/Old_Menu_HLT.py, Rates/ignore.py = Prod/ignore.py,<br/>Rates/filesInput.py, filesInputData_old.py, aux.pyc,<br/>an editor autosave copy of mergeOutputs.py"]
        L5["list_cff.py (repo root), Rates/list_cff_SkimD.py"]
    end

    classDef script fill:#d0ebff,stroke:#1c7ed6,color:#000
    classDef broken fill:#ffe3e3,stroke:#e03131,stroke-dasharray: 5 5,color:#000
    class HD,CD,AV,OV script
    class W1,W2,W3,L1,L2,L3,L4,L5 broken
```

---

## 9. Where every user setting lives

Everything below is the input for the planned "single configuration file".

| Setting | Where it is set today | Notes |
|---|---|---|
| HLT menu, global tag, prescale column, process name | `hltGetConfiguration` command line (README) | process name `MYHLT` is also hardcoded in `run_steamflow_cfg.py` and the counter |
| L1 seed renames / disables | dicts inside `Prod/L1Seed*.py`, `Prod/L1Workaround.py` | edit `hlt.py` in place |
| RAW input datasets, run, LS range | `Prod/createFileList.py` (top of file) | output file name also hardcoded |
| which file list to run on | `from list_cff_... import` in `Prod/run_steamflow_cfg.py` | |
| nEvents, switchL1PS, columnL1PS, output name | `Prod/run_steamflow_cfg.py` | nEvents must be reset to -1 after the local test |
| CMSSW dir, output dir, files/job, flavour, proxy | `cmsCondorData.py` command line | |
| ntuple directory, CMSSW dir, file_type | `Rates/config_makeCondorJobsData.py` | |
| lumi JSON | `Rates/config_makeCondorJobsData.py` → `Rates/Json/*.txt` | |
| maps mode | `config_makeCondorJobsData.py` **and** `config_mergeOutputsData.py` | must be kept consistent by hand |
| files/job, flavour (counting) | `config_makeCondorJobsData.py` (`isUnusual`, `n`, `flavour`) | |
| lumi_in, lumi_target, hlt_ps | `Rates/config_mergeOutputsData.py` | history of values kept as comments |
| makeFigures, input dir for merging | `Rates/config_mergeOutputsData.py` | |
| paths excluded from pure rates | `Rates/triggerstoexclude.py` | |
| path → dataset/stream/group/type | `Rates/Menu_HLT.py` (generated in `STEAM_maps/`, copied by hand) | |
| stream classification rules (what counts as physics/scouting/parking) | string rules inside `Rates/aux.py` | |
| PAG list, datasets for correlation plots | `Rates/aux.py` (`PAGlist`, `datasets_for_corr`) | `datasets_for_corr` still uses Run-2 names |
| LS length 23.31 s | `Rates/mergeOutputs.py` | |
| MC cross sections | `MCDatasets/map_MCdatasets_xs.py` | |
| target lumi (MC) | `Rates/config_mergeOutputsMC.py` | |

---

## Known issues

Performance (mostly relevant for `maps` mode):
1. `check_json` re-opens and re-parses the lumi JSON for **every event**.
2. The list `tnames` is extended by all excluded triggers on **every event** and never reset (`triggerCountsFromTriggerResults.py:368-369`). Memory grows and the `in tnames` check gets slower with every event.
3. `physicsStreamOK` / `scoutingStreamOK` / `parkingStreamOK` / `belongsToPAG` (`aux.py`) scan all ~750 map entries (with `rstrip`) for every fired path in every event. Several `x in list(d.keys())` checks add to this. This is the main reason maps are slow.

Correctness / robustness:

4. Version stripping with `rstrip("0123456789")` also removes digits that belong to the name. `Dataset_ScoutingPF0` and `Dataset_ScoutingPF1` collide, and `EGamma0` becomes `EGamma`.
5. "Pure" is defined against different path sets in `nomaps` and maps mode (see section 4).
6. `maps = allmaps` crashes, because `newDatasetMap` exists in no `Menu_HLT.py`. `compareWBM.py` and `adjustPS.py` import maps that do not exist.
7. **Duplicate files in input lists.** `Prod/createFileList.py` queries DAS once per lumi section and appends all results without deduplication. A RAW file that contains several LS is therefore listed several times. Commit 68d5b60 removed such duplicates by hand.
8. **N_LS normalization relies on implicit assumptions.** N_LS is counted per job and summed without deduplication. This is correct only if each (dataset, LS) pair is in exactly one ntuple file, and if `hlt_ps` contains the number of parallel datasets (factor 8 above).
   - Duplicated input files double both counts and N_LS. This wastes CPU and fakes statistics.
   - An LS split across two files is counted twice, which lowers the rates.
9. Bad-job detection in `aux.makeListsOfRawOutputs` matches job numbers as substrings of file names.
10. MC is detected by checking whether the `-j` argument looks like a JSON filename (`triggerCountsFromTriggerResults.py:126`).

Workflow / structure:

11. Shared mutable state (`Jobs/`, `filesInputData.py`, `Results/`, `sub_total.jobb`, `condor_cluster.sub`): one measurement per checkout at a time, and old results get wiped.
12. Settings are spread over ~10 files (section 9). The maps mode must be set consistently in two places.
13. MC and data scripts are copy-pasted pairs (section 7).
14. Hardcoded leftovers: `metBx` / `muonBx` histograms in every job, LS length, PAG and dataset lists, process names.
15. Output is CSV with mislabelled columns. Results live in relative paths inside the code checkout.

Capabilities that are missing today:

16. **No L1 rates**: the ntuples contain no L1 information.
17. **No per-module / per-filter rates.** However, `edm::TriggerResults` already stores, for each path, the index of the last module that ran (`HLTPathStatus::index()`). Together with the module list of each path from the menu, this would give filter-level efficiencies from the existing ntuples.

---

## Design principles for the refactor

Collected from the project goals; to be refined.

- **One configuration file per measurement**, and **one working directory per measurement**. No shared state, so several measurements can run in parallel.
- **Data and MC share one pipeline.** A sample's kind only selects
  - (a) the event selection (lumi JSON or none) and
  - (b) the normalization strategy: *data*: luminosity × prescale / (N_LS × t_LS); *MC*: cross section × filter efficiency × lumi / N_events (or the sum of weights).
- **MC first-class**: MC will be the main use case for HL-LHC / Phase-2 studies. This means per-sample cross sections, combining several samples (e.g. pT-binned QCD) into one rate, and possibly event weights.
- **Input layer decoupled from counting**, so the current STEAM ntuples and the upcoming common L1+HLT ntuple format can both be read (likely with uproot/awkward, which are available in the CMSSW environment).
- **L1 and HLT rates**, plus optional per-module / per-filter rates.
- **JSON outputs** with metadata (config, menu, run, normalization inputs), ready for a future comparison website. Possibly runnable by a web service later, so no interactive or lxplus-only assumptions in the core.
- **Fast correlation / overlap studies**, with flexible, nice-looking default plots.
- Input file lists are **deduplicated and validated** before jobs are created. Normalization inputs are explicit and checked.
