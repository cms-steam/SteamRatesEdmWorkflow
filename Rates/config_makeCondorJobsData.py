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
#inputFilesDir = "/eos/cms/store/group/dpg_trigger/comm_trigger/TriggerStudiesGroup/STEAM/dbeghin/NoCustom"
inputFilesDir = "/afs/cern.ch/work/s/savarghe/private/RateValidation/CMSSW_12_0_1/src/SteamRatesEdmWorkflow/Prod/tuples"
#Were your input files produced by STEAM? If yes, file_type = "custom"
#Are these raw data files?
#If yes, are you running specifically on L1Accept files? Then file_type = "L1Accept"
#Are you running on other non-L1Accept data files? Then file_type = "RAW"
file_type = "custom"
#file_type = "RAW"
#file_type = "L1Accept"

#Directory where the top of your CMSSW release is located
#cmsswDir = "/afs/cern.ch/work/d/dbeghin/Work/Rates/2020NewPF/NoCustom/CMSSW_11_1_0_pre4/src"
cmsswDir = "/afs/cern.ch/work/s/savarghe/private/RateValidation/CMSSW_12_0_1/src"
#Json file
# json_file = "/afs/cern.ch/work/d/dbeghin/public/json/json_323775.txt"
#json_file = "/afs/cern.ch/work/s/savarghe/public/L1Skim/myRealjson.txt"
json_file = "/afs/cern.ch/work/s/savarghe/public/HLTRatesFiles/json_323775.txt"

#Do you wish to use the dataset/group/etc. maps? The maps are unnecessary if you're an HLT developer and you're just testing your new path rate.
#If you don't want to use any maps, set the variable below to "nomaps"
maps = "nomaps" #recommended if you're an HLT dev
#maps = "somemaps" #if you want dataset/group/etc. rates but no dataset merging study
#maps = "allmaps" #if you want to study dataset merging

#Do you wish to use any unusual (non-default) options for the job flavour, and the number of files processed per job?
#If you do, set the following boolean to True
isUnusual = True
#If you do, please also specify the following parameters:
#number of files processed per job
n = 1
#Job flavour
#flavour = "microcentury"
flavour = "espresso"
'''
--------------------------OPTIONS TO BE FILLED OUT-----------------------------------------
'''


#run the script
command = ""
if makeInputFilesList:
    command = "python3 condorScriptForRatesData.py -j %s -e %s -i %s -f %s" %(json_file, cmsswDir, inputFilesDir, file_type)
else:
    command = "python3 condorScriptForRatesData.py -j %s -e %s -f %s" %(json_file, cmsswDir, file_type)

if isUnusual:
    command += " -n %s -q %s" %(n, flavour)

command += " -m %s" %maps
os.system(command)






