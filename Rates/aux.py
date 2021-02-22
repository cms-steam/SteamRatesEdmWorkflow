"""
Just a list of auxiliary functions used by scripts in this directory
"""

import os
import math
import shlex
import subprocess
import csv
import ROOT
#from Menu_HLT import datasetMap as triggersDatasetMap

#List of datasets to be used for the overlap study:
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

#Dictionary relating each category of output file with the directory where they can be found
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


#List of PAGs (used for the "total physics analysis" rate)
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
    #run a shell command (like you would run on a terminal) and return the output
    #sys.stdout.write("%s\n" % commandLine)
    args = shlex.split(commandLine)
    retVal = subprocess.Popen(args, stdout = subprocess.PIPE)
    return retVal


def belongsToPAG(triggerName):
    #Check if a given trigger name belongs to a PAG, using the PAG list
    from Menu_HLT import groupMap as triggersGroupMap
    result=False
    for mapKey in triggersGroupMap.keys():
        if result: break
        if triggerName == mapKey.rstrip("0123456789"):
            for group in triggersGroupMap[mapKey]:
                if group in PAGlist:
                    result = True
                    break
    return result

def physicsStreamOK(triggerName):
    #Check if a given trigger belongs to a "physics" stream (defined below)
    from Menu_HLT import streamMap as triggersStreamMap
    result=False
    for mapKey in triggersStreamMap.keys():
        if result: break
        if triggerName == mapKey.rstrip("0123456789"):
            for stream in triggersStreamMap[mapKey]:
                #"physics" streams are defined in the "if" statement below:
                if (stream.startswith("Physics")) and not (stream.startswith("PhysicsHLTPhysics")) and not (stream.startswith("PhysicsZeroBias")) and not (stream.startswith("PhysicsParking")) and not (stream.startswith("PhysicsScoutingMonitor")):
                    result = True
    return result

def scoutingStreamOK(triggerName):
    #Check if a given trigger belongs to a "scouting" stream (defined below)
    from Menu_HLT import streamMap as triggersStreamMap
    result=False
    for mapKey in triggersStreamMap.keys():
        if result: break
        if triggerName == mapKey.rstrip("0123456789"):
            for stream in triggersStreamMap[mapKey]:
                if (stream.startswith("Scouting")):
                    result = True
    return result

def parkingStreamOK(triggerName):
    #Check if a given trigger belongs to a "parking" stream (defined below)
    from Menu_HLT import streamMap as triggersStreamMap
    result=False
    for mapKey in triggersStreamMap.keys():
        if result: break
        if triggerName == mapKey.rstrip("0123456789"):
            for stream in triggersStreamMap[mapKey]:
                if ("Parking" in stream):
                    result = True
    return result

def datasetOK(dataset):
    #check if a given dataset belongs to the list to be considered for the correlation study
    result=False
    if dataset in datasets_for_corr: result=True
    return result


def makeIncreasingList(map_in):
    #function that takes in a map and returns a list of keys of the map, ordered from lowest to highest
    #this can be replaced by the native Python function "sorted" which does this better
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
    #Obsolete?
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
    #Obsolete?
    newList = []
    for j in range(0,len(list_in)):
        #j is the new index
        old_index = reorderingMap[j]
        if old_index >= len(list_in):
            print "Reordering of the list\n", list_in, "\nfailed : the map provided yields an out of range index\n", str(old_index) + " >= " + str(len(list_in))
            break
        newList.append(list_in[old_index])
    return newList


def findFileNumber(sstring):
    #Functions that takes the full name of a file and finds the tag assigned at the end
    #The full name of a file is something like 'output.category.tag.csv' or 'output.category.tag.root'
    #This functions looks for the tag by looking between the last and second to last points
    #The tag is called "file number" because it will be a number if you created the condor jobs using the automated scripts
    lastpoint = sstring.rfind('.')
    secondlastpoint = sstring.rfind('.', 0, lastpoint-1)
    file_number = sstring[secondlastpoint:lastpoint+1]
    return file_number
    

def makeListsOfRawOutputs(files_dir, fig):
    #Makes lists of the output files for the counting jobs
    #One list for each category of output
    #Categories="global", "root", "physics path", "group", etc.

    #First we try to find the jobs where the output is corrupt
    bad_jobs=[]

    #search for corrupt global files
    total_dir = files_dir + "/Global"
    ls_command = runCommand("ls " + total_dir)
    stdout, stderr = ls_command.communicate()
    for line in stdout.splitlines():
        file_string = total_dir + "/" + line
        try:
            with open(file_string) as ffile:
                reader=csv.reader(ffile, delimiter=',')
                for row in reader:
                    r = row[0]
                    break
        except:
            bad_jobs.append(findFileNumber(file_string))

    #search for corrupt root files, but only if we specify the "fig" option
    if fig:
        total_dir = files_dir + "/Root"
        ls_command = runCommand("ls " + total_dir)
        stdout, stderr = ls_command.communicate()
        for line in stdout.splitlines():
            file_string = total_dir + "/" + line
            ffile = ROOT.TFile(file_string,"R")
            if ffile.IsZombie() or ffile.TestBit(ROOT.TFile.kRecovered):
                nnumber = findFileNumber(file_string)
                if not nnumber in bad_jobs:
                    bad_jobs.append(nnumber)

    #search for corrupt files in all the other categories
    for name in mergeNames:
        key = name+".csv"

        total_dir = files_dir + "/" + mergeNames[name]
        ls_command = runCommand("ls " + total_dir )
        stdout, stderr = ls_command.communicate()
        for line in stdout.splitlines():
            file_string = total_dir + "/" + line
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


    print "bad jobs =", bad_jobs

    #Now we create the lists of output files that will need to be merged
    #The "global" and "root" files are special and put in their own separate lists
    globalFiles = []
    total_dir = files_dir + "/Global"
    ls_command = runCommand("ls " + total_dir)
    stdout, stderr = ls_command.communicate()
    for line in stdout.splitlines():
        file_string = total_dir + "/" + line
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
            file_string = total_dir + "/" + line
            bad = False
            for number in bad_jobs:
                if number in file_string:
                    bad = True
                    break
            if not bad:
                rootFiles.append(file_string)
    
    #For categories other than "root" or "global", we put the lists in a dictionary
    masterDic = {}
    for name in mergeNames:
        key = name+".csv"
        masterDic[key] = []

        total_dir = files_dir + "/" + mergeNames[name]
        ls_command = runCommand("ls " + total_dir )
        stdout, stderr = ls_command.communicate()
        for line in stdout.splitlines():
            file_string = total_dir + "/" + line
            bad = False
            for number in bad_jobs:
                if number in file_string:
                    bad = True
                    break
            if not bad:
                masterDic[key].append(file_string)


    return masterDic, rootFiles, globalFiles
