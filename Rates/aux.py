import os
import math
import shlex
import subprocess
#from Menu_HLT import datasetMap as triggersDatasetMap

datasets_for_corr=[
"NoBPTX",
"DoubleEG",
"SingleElectron",
"SinglePhoton",
"BTagCSV",
"BTagMu",
"DisplacedJet",
"EGamma",
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
"output.stream"               : "Stream",
"output.path.physics"         : "PathPhysics",
"output.path.scouting"        : "PathScouting",
"output.path.parking"         : "PathParking",
"output.path.misc"            : "PathMisc",
"output.dataset.physics"      : "DatasetPhysics",
"output.newDataset.physics"   : "NewDatasetPhysics",
"output.dataset.misc"         : "DatasetMisc",
"output.group"                : "Group",
"output.dataset_dataset_corr" : "DatasetDatasetCorr",
"output.trigger_dataset_corr" : "TriggerDatasetCorr",
"output.newDataset_newDataset_corr" : "NewDatasetDatasetCorr",
"output.trigger_newDataset_corr" : "NewTriggerDatasetCorr",
}


def runCommand(commandLine):
    #sys.stdout.write("%s\n" % commandLine)
    args = shlex.split(commandLine)
    retVal = subprocess.Popen(args, stdout = subprocess.PIPE)
    return retVal


def physicsStreamOK(triggerName):
    from Menu_HLT import streamMap as triggersStreamMap
    result=False
    for mapKey in triggersStreamMap.keys():
        if triggerName == mapKey.rstrip("0123456789"):
            for stream in triggersStreamMap[mapKey]:
                if (stream.startswith("Physics")) and not (stream.startswith("PhysicsHLTPhysics")) and not (stream.startswith("PhysicsZeroBias")) and not (stream.startswith("PhysicsParking")):
                    result = True
    return result

def scoutingStreamOK(triggerName):
    from Menu_HLT import streamMap as triggersStreamMap
    result=False
    for mapKey in triggersStreamMap.keys():
        if triggerName == mapKey.rstrip("0123456789"):
            for stream in triggersStreamMap[mapKey]:
                if (stream.startswith("Scouting")):
                    result = True
    return result

def parkingStreamOK(triggerName):
    from Menu_HLT import streamMap as triggersStreamMap
    result=False
    for mapKey in triggersStreamMap.keys():
        if triggerName == mapKey.rstrip("0123456789"):
            for stream in triggersStreamMap[triggerName]:
                if ("Parking" in stream):
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
    

def mapForDecreasingOrder(list_in):
    newToOldMap = {}
    llist = list(list_in)
    processed_elements = []
    for j in range(0,len(llist)):
        mmax = -1
        max_index = -1
        for i in range(0, len(llist)):
            if i in processed_elements: continue
            if llist[i] > mmax:
                mmax = llist[i]
                max_index = i
        newToOldMap[j] = max_index
        processed_elements.append(max_index)
        

    return newToOldMap

def reorderList(list_in, reorderingMap):
    newList = []
    for j in range(0,len(list_in)):
        #j is the new index
        old_index = reorderingMap[j]
        if old_index >= len(list_in):
            print "Reordering of the list\n", list_in, "\nfailed : the map provided yields an out of range index\n", str(old_index) + " >= " + str(len(list_in))
            break
        newList.append(list_in[old_index])
    return newList


def makeListsOfRawOutputs(files_dir):

    masterDic = {}
    for name in mergeNames:
        key = name+".csv"
        masterDic[key] = []

        total_dir = files_dir + "/" + mergeNames[name]
        ls_command = runCommand("ls " + total_dir )
        stdout, stderr = ls_command.communicate()
        for line in stdout.splitlines():
            masterDic[key].append(total_dir + "/" + line)


    rootFiles = []
    total_dir = files_dir + "/Root"
    ls_command = runCommand("ls " + total_dir)
    stdout, stderr = ls_command.communicate()
    for line in stdout.splitlines():
        rootFiles.append(total_dir + "/" + line)


    globalFiles = []
    total_dir = files_dir + "/Global"
    ls_command = runCommand("ls " + total_dir)
    stdout, stderr = ls_command.communicate()
    for line in stdout.splitlines():
        globalFiles.append(total_dir + "/" + line)

    return masterDic, rootFiles, globalFiles
