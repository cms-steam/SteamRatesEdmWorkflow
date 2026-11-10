# SteamRatesEdmWorkflow
Old Documentation: Twiki https://twiki.cern.ch/twiki/bin/viewauth/CMS/SteamHLTRatesCalculation#Running_on_Run3_HLTPhysics_Data
New Documentation: https://cms-hlt-steam.docs.cern.ch/Rates/RatesGeneral/#create-and-run-the-jobs

# Workflow overview

https://github.com/cms-steam/SteamRatesEdmWorkflow/wiki

# Exampel Recipe (Run 3 version)
This is just a simple recipe. For more informaton check the documenation page linked above.
## Setup

```
cmsrel CMSSW_16_0_6_patch1
cd CMSSW_16_0_6_patch1/src/
cmsenv
scram b -j 4
git clone https://github.com/cms-steam/SteamRatesEdmWorkflow.git
```

## Production of HLT ntuples 

```
cd SteamRatesEdmWorkflow/Prod/
```

Set up the config
```
hltGetConfiguration /dev/CMSSW_16_0_0/GRun --full --offline --no-output --data --process MYHLT --type GRun --prescale 2p0E34+ZeroBias+HLTPhysics --globaltag 160X_dataRun3_HLT_v1 --max-events -1  > hlt.py
edmConfigDump hlt.py > hlt_config.py
```

Edit `run_steamflow_cfg.py` to specify the sample list. In this example we use Run 403663 for which the files are listed in `list_cff_EHP_403663.py`
Also good idea to test that the jobs work locally. For that temporarilly change the number of events to something like 500 and then run 
```
cmsRun run_steamflow_cfg.py
```
Do not forget to change the number of events back to -1 before continuing.

Submit the jobs 
```
voms-proxy-init --voms cms --valid 168:00
cp /tmp/x509up_<user proxy> /afs/cern.ch/user/<letter>/<username>/private/ 

./cmsCondorData.py run_steamflow_cfg.py  <path to your CMSSW src directory>  <path to your output directory>  -n 1 -q <Job Flavour> -p /afs/cern.ch/user/<first letter>/<username>/private/x509up_<user proxy>
./sub_total.jobb
```

Now wait until the jobs are done (condor_q)

## Run counting scripts
```
cd ./../Rates
```
Now edit `config_makeCondorJobsData.py` and specify:
- The location of the ntuples from the previous step
- the lumi filter json. (Here: json_403663.txt) 
- path to CMSSW
- the maps option (default: "nomaps")

Create the job scripts and submit
```
python3 config_makeCondorJobsData.py
./sub_total.jobb
```

Once the jobs are done we can merge the outputs and produce the final results. For this, edit `config_mergeOutputsData.py` and specify the 
- `lumi_in` # In our case 2.2
- `lumi_target`  # In our case also 2.2
- `hlt_ps`  # In our case 1760

Then finally run the merging script like 
```
python3 config_mergeOutputsData.py
```

The results will be found in `Results/Data/`

