import os
import math
import shlex
import subprocess
from Menu_HLT import streamMap as triggersStreamMap
#from Menu_HLT import datasetMap as triggersDatasetMap

datasets_for_corr=[
"NoBPTX",
"DoubleEG",
"SingleElectron",
"SinglePhoton",
"BTagCSV",
"BTagMu",
"DisplacedJet",
"HTMHT",
"JetHT",
"MET",
"Tau",
"Charmonium",
"DoubleMuon",
"DoubleMuonLowMass",
"MuOnia",
"MuonEG",
"SingleMuon",
]

#Dictionary relating rates output files with their directories
mergeNames = {
"output.path.physics"         : "PathPhysics",
"output.path.scouting"        : "PathScouting",
"output.dataset.physics"      : "DatasetPhysics",
"output.dataset.scouting"     : "DatasetScouting",
"output.group"                : "Group",
"output.stream"               : "Stream",
"output.dataset_dataset_corr" : "DatasetDatasetCorr",
"output.trigger_dataset_corr" : "TriggerDatasetCorr",
}


def runCommand(commandLine):
    #sys.stdout.write("%s\n" % commandLine)
    args = shlex.split(commandLine)
    retVal = subprocess.Popen(args, stdout = subprocess.PIPE)
    return retVal


def physicsStreamOK(triggerName):
    result=False
    if triggerName in triggersStreamMap.keys():
        for stream in triggersStreamMap[triggerName]:
            if (stream.startswith("Physics")) and not (stream.startswith("PhysicsHLTPhysics")) and not (stream.startswith("PhysicsZeroBias")) and not (stream.startswith("PhysicsParking")) and not (stream.startswith("PhysicsCommissioning")):
                result = True
    return result

def scoutingStreamOK(triggerName):
    result=False
    if triggerName in triggersStreamMap.keys():
        for stream in triggersStreamMap[triggerName]:
            if (stream.startswith("Scouting")):
                result = True
    return result

def datasetOK(dataset):
    result=False
    if dataset in datasets_for_corr: result=True
    return result


def makeIncreasingList(map_in):
    sorted_list = []
    mmap = map_in.copy()
    while len(mmap) > 0:
        mmin = 9000000000
        min_key = ""
        for key in mmap:
            if mmap[key] <= mmin:
                mmin = mmap[key]
                min_key = key
        sorted_list.append(min_key)
        del mmap[min_key]

    return sorted_list
    
