'''
This file allows you to produce a set of jobs to be sent to HT Condor.
The jobs will be located in the 'Jobs' directory.
After producing them, it is advisable to test one of the jobs locally before sending them all.

Example of a local test:
./Jobs/sub_job/sub_0.jobb

Submitting all jobs on batch:
./sub_total.jobb
'''

import os
import sys


'''
--------------------------OPTIONS TO BE FILLED OUT-----------------------------------------
'''
#If you already have a list of input files with the proper format, you may not want to remake it
#Type True if you want to remake them, False otherwise
makeInputFilesList = True
#Directory where your input root files are located
inputFilesDir = "/eos/home-s/savarghe/QCD_Pt-120To170_EMEnriched_TuneCP5_14TeV-pythia8_Run3Winter21DRMiniAOD-FlatPU30to80FEVT_112X_mcRun3_2021_realistic_v16-v2_GEN-SIM-DIGI-RAW"

#Directory where the top of your CMSSW release is located
cmsswDir = "/afs/cern.ch/work/s/savarghe/private/HLTratesMC/CMSSW_12_0_2_patch1/src/"

#Do you wish to use the dataset/group/etc. maps? The maps are unnecessary if you're an HLT developer and you're just testing your new path rate.
#If you don't want to use any maps, set the variable below to "nomaps"
#maps = "nomaps" #recommended if you're an HLT dev
maps = "somemaps" #if you want dataset/group/etc. rates but no dataset merging study
#maps = "allmaps" #if you want to study dataset merging

#Do you wish to use any unusual (non-default) options for the job flavour, and the number of files processed per job?
#If you do, set the following boolean to True
isUnusual = True
#If you do, please also specify the following parameters:
#number of files processed per job
n = 1
#Job flavour
flavour = "espresso"
'''
--------------------------OPTIONS TO BE FILLED OUT-----------------------------------------
'''


#run the script
command = ""
if makeInputFilesList:
    command = "python3 condorScriptForRatesMC.py -e %s -i %s" %(cmsswDir, inputFilesDir)
else:
    command = "python3 condorScriptForRatesMC.py -e %s" %cmsswDir

if isUnusual:
    command += " -n %s -q %s" %(n, flavour)

command += " -m %s" %maps
os.system(command)






