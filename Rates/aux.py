import os
import math
import shlex
import subprocess
import csv
import ROOT
#from Menu_HLT import datasetMap as triggersDatasetMap

datasets_for_corr=[
"NoBPTX",
#"DoubleEG",
#"SingleElectron",
#"SinglePhoton",
#"BTagCSV",
"BTagMu",
"DisplacedJet",
"EGamma",
#"HTMHT",
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


PAGlist=[
"B2G",
"BPH",
"EXO",
"HIG",
"SMP",
"SUS",
"TOP",
"FSQ",
"HIN",
]


def runCommand(commandLine):
    #sys.stdout.write("%s\n" % commandLine)
    args = shlex.split(commandLine)
    retVal = subprocess.Popen(args, stdout = subprocess.PIPE)
    return retVal


def belongsToPAG(triggerName):
    from Menu_HLT import groupMap as triggersGroupMap
    result=False
    for mapKey in list(triggersGroupMap.keys()):
        if result: break
        if triggerName == mapKey.rstrip("0123456789"):
            for group in triggersGroupMap[mapKey]:
                if group in PAGlist:
                    result = True
                    break
    return result

def physicsStreamOK(triggerName):
    from Menu_HLT import streamMap as triggersStreamMap
    result=False
    for mapKey in list(triggersStreamMap.keys()):
        if result: break
        if triggerName == mapKey.rstrip("0123456789"):
            for stream in triggersStreamMap[mapKey]:
                if (stream.startswith("Physics")) and not (stream.startswith("PhysicsHLTPhysics")) and not (stream.startswith("PhysicsZeroBias")) and not (stream.startswith("PhysicsParking")) and not (stream.startswith("PhysicsScoutingMonitor")) and not (stream.startswith("PhysicsScoutingPFMonitor")):
                    result = True
    return result

def scoutingStreamOK(triggerName):
    from Menu_HLT import streamMap as triggersStreamMap
    result=False
    for mapKey in list(triggersStreamMap.keys()):
        if result: break
        if triggerName == mapKey.rstrip("0123456789"):
            for stream in triggersStreamMap[mapKey]:
                if (stream.startswith("Scouting")):
                    result = True
    return result

def parkingStreamOK(triggerName):
    from Menu_HLT import streamMap as triggersStreamMap
    result=False
    for mapKey in list(triggersStreamMap.keys()):
        if result: break
        if triggerName == mapKey.rstrip("0123456789"):
            for stream in triggersStreamMap[mapKey]:
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
            print(("Reordering of the list\n", list_in, "\nfailed : the map provided yields an out of range index\n", str(old_index) + " >= " + str(len(list_in))))
            break
        newList.append(list_in[old_index])
    return newList


def findFileNumber(sstring):
    lastpoint = sstring.rfind('.')
    secondlastpoint = sstring.rfind('.', 0, lastpoint-1)
    file_number = sstring[secondlastpoint:lastpoint+1]
    return file_number
    

def makeListsOfRawOutputs(files_dir, fig):

    bad_jobs=[]

    total_dir = files_dir + "/Global"
    ls_command = runCommand("ls " + total_dir)
    stdout, stderr = ls_command.communicate()
    for line in stdout.splitlines():
        newline = line.decode('ascii')
        file_string = str(total_dir + '/' + newline)
        #print(file_string)
        try:
            with open(file_string) as ffile:
                #print(ffile)
                reader=csv.reader(ffile, delimiter=',')
                for row in reader:
                    r = row[0]
                    break
        except:
            bad_jobs.append(findFileNumber(file_string))

    if fig:
        total_dir = files_dir + "/Root"
        ls_command = runCommand("ls " + total_dir)
        stdout, stderr = ls_command.communicate()
        for line in stdout.splitlines():
            newline = str(line).replace("b'","")
            newline = newline.replace("'","")
            file_string = total_dir + "/" + newline
           # print(file_string)
            ffile = ROOT.TFile(file_string,"R")
            if ffile.IsZombie() or ffile.TestBit(ROOT.TFile.kRecovered):
                nnumber = findFileNumber(file_string)
                if not nnumber in bad_jobs:
                    bad_jobs.append(nnumber)

    for name in mergeNames:
        key = name+".csv"

        total_dir = files_dir + "/" + mergeNames[name]
        ls_command = runCommand("ls " + total_dir )
        stdout, stderr = ls_command.communicate()
        for line in stdout.splitlines():
            newline = str(line).replace("b'","")
            newline = newline.replace("'","")
            file_string = total_dir + "/" + newline
            try:
                with open(file_string) as ffile:
                    reader=csv.reader(ffile, delimiter=',')
                    for row in reader:
                        r = row[1]
                        break
            except:
                nnumber = findFileNumber(file_string)
                if not nnumber in bad_jobs:
                    bad_jobs.append(nnumber)


    print(("bad jobs =", bad_jobs))
    globalFiles = []
    total_dir = files_dir + "/Global"
    ls_command = runCommand("ls " + total_dir)
    stdout, stderr = ls_command.communicate()
    for line in stdout.splitlines():
        newline = str(line).replace("b'","")
        newline = newline.replace("'","")
        file_string = total_dir + "/" + newline
        bad = False
        for number in bad_jobs:
            if number in file_string:
                bad = True
                break
        if not bad:
            globalFiles.append(file_string)

    rootFiles = []
    if fig:
        total_dir = files_dir + "/Root"
        ls_command = runCommand("ls " + total_dir)
        stdout, stderr = ls_command.communicate()
        for line in stdout.splitlines():
            newline = str(line).replace("b'","")
            newline = newline.replace("'","")
            file_string = total_dir + "/" + newline
            bad = False
            for number in bad_jobs:
                if number in file_string:
                    bad = True
                    break
            if not bad:
                rootFiles.append(file_string)
    
    masterDic = {}
    for name in mergeNames:
        key = name+".csv"
        masterDic[key] = []

        total_dir = files_dir + "/" + mergeNames[name]
        ls_command = runCommand("ls " + total_dir )
        stdout, stderr = ls_command.communicate()
        for line in stdout.splitlines():
            newline = str(line).replace("b'","")
            newline = newline.replace("'","")
            file_string = total_dir + "/" + newline
            bad = False
            for number in bad_jobs:
                if number in file_string:
                    bad = True
                    break
            if not bad:
                masterDic[key].append(file_string)


    return masterDic, rootFiles, globalFiles
