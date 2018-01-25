import ROOT
from DataFormats.FWLite import Handle, Events
import math
import json
import sys, getopt
import os
#from filesInput import fileInputNames
from aux import physicsStreamOK
from aux import scoutingStreamOK
from aux import datasets_for_corr as good_datasets
from aux import makeIncreasingList

#list of input files
#json_file = '/afs/cern.ch/user/n/ndaci/public/STEAM/Production/Cheng_HLTv4p1/json_DCS_305636_1.5e34_Reduced.txt'
#json_file = '/afs/cern.ch/user/n/ndaci/public/STEAM/JSON/ProcessL1Accept2017/json_306154_PS6_10LS.txt'
#json_file = '/afs/cern.ch/user/n/ndaci/public/STEAM/JSON/ProcessL1Accept2017/json_306154_PS2_10LS.txt'
json_file = '/afs/cern.ch/user/n/ndaci/public/STEAM/JSON/ProcessL1Accept2017/json_306154_PS7_10LS.txt'

#auxiliary functions


def checkTriggerIndex(name,index, names):
    if not 'firstTriggerError' in globals():
        global firstTriggerError
        firstTriggerError = True
    if index>=names.size():
        if firstTriggerError:
            for tr in names: print tr
            print
            print name," not found!"
            print
            firstTriggerError = False
            return False
        else:
            return False
    else:
        return True

def check_json(jsonf, runNo_in, LS):
    runNo = str(runNo_in)
    file1=open(jsonf,'r')
    inp1={}
    text = ""
    for line1 in file1:
        text+=line1
    inp1 = json.loads(text)
    #print inp1.keys()
    if runNo in inp1:
        for part_LS in inp1[runNo]:
            if LS >= part_LS[0] and LS <= part_LS[1]:
                return True
    return False


#Dataset
from Menu_HLT import groupMap as triggersGroupMap
from Menu_HLT import datasetMap as  triggersDatasetMap
from Menu_HLT import streamMap as  triggersStreamMap



triggerList = []
primaryDatasetList = []
primaryDatasetCounts = {}
datasets = {}

groupList = []
groupCounts = {}
groupCountsShared = {}
groupCountsPure = {}
groups = {}

streamList = []
streamCounts = {}

#make output dir
from aux import mergeNames

try:
    os.system("mkdir Results")
    os.system("mkdir Results/Raw")
    for name in mergeNames:
        os.system("mkdir Results/Raw/"+mergeNames[name])
    os.system("mkdir Results/Raw/Root")
    os.system("mkdir Results/Raw/Global")
except:
    print "err!"
    pass


#get inputs
from optparse import OptionParser
parser=OptionParser()
parser.add_option("-i","--infile",dest="inputFile",type="str",default="noroot",help="one (1) input root FILE",metavar="FILE")
parser.add_option("-j","--json",dest="jsonFile",type="str",default="nojson",help="text FILE with the LS range in json format",metavar="FILE")
parser.add_option("-f","--filetype",dest="fileType",type="str",default="custom",help="ARG='custom' (default option) or 'RAW', use 'custom' if you're running on STEAM-made files, 'RAW' if you're running on raw data",metavar="ARG")

opts, args = parser.parse_args()


error_text = '\n\nError: wrong inputs\n'
help_text = '\npython triggerRatesFromTriggerResults.py -i <inputfile> -j <json> -f <filetype>'
help_text += '\n<inputfile> (mandatory argument) = one (1) input root file'
help_text += '\n<json> (mandatory) = text file with the LS range in json format'
help_text += '\n<filetype> (optional) = "custom" (default option) or "RAW"\n'

if opts.inputFile == "noroot" or opts.jsonFile == "nojson":
    print error_text
    print help_text
    sys.exit(2)


isRawFiles = False
if opts.fileType == "custom":
    isRawFiles = False
elif opts.fileType == "RAW":
    isRawFiles = True
else:
    print error_text
    print help_text
    sys.exit(2)

final_string = opts.inputFile[-9:-5]
if isRawFiles:
    final_string = opts.inputFile[-22:-5]

#Handles and labels
if isRawFiles:
    triggerBits, triggerBitLabel = Handle("edm::TriggerResults"), ("TriggerResults::HLT")
else:
    triggerBits, triggerBitLabel = Handle("edm::TriggerResults"), ("TriggerResults::MYHLT")


#Looping over the inputfiles
n = 0
nPassed_Physics = 0
nPassed_Scouting = 0

#List of triggers
myPaths = []
reducedDatasets = []
myPassedEvents = {}

nLS = 0


triggerDatasetCorrMatrix = {}
datasetDatasetCorrMatrix = {}



#get rates from input file
events = Events (opts.inputFile)

#Looping over events in inputfile

runAndLsList = []
atLeastOneEvent = False
for event in events: 
    #if n == 100000: break
    #taking trigger informations: names, bits and products
    event.getByLabel(triggerBitLabel, triggerBits)
    names = event.object().triggerNames(triggerBits.product())    
    runnbr = event.object().id().run()
    runls = event.object().id().luminosityBlock()
    runstr = str((runnbr,runls))
    if not check_json(opts.jsonFile, runnbr, runls):
        continue


    if not runstr in runAndLsList:
        nLS = nLS +1
        runAndLsList.append(runstr)

    #initializing stuff
    if n<1:
        for name in names.triggerNames():
            name = str(name)
            if ("HLTriggerFirstPath" in name) or ("HLTriggerFinalPath" in name): continue
            if not (name.startswith("HLT_") or name.startswith("DST_")): continue
            myPaths.append(name)
            triggerKey = name.rstrip("0123456789")
            if not triggerKey in triggersDatasetMap: continue
            datasets.update({str(triggerKey):triggersDatasetMap[triggerKey]})
            groups.update({str(triggerKey):triggersGroupMap[triggerKey]})
            if not (name in triggerList) :triggerList.append(name)
            for dataset in triggersDatasetMap[triggerKey]:
                if not dataset in primaryDatasetList: primaryDatasetCounts.update({str(dataset):0}) 
                if not dataset in primaryDatasetList: primaryDatasetList.append(dataset)
            for group in triggersGroupMap[triggerKey]:
                if not group in groupList: groupCounts.update({str(group):0}) 
                if not group in groupList: groupCountsShared.update({str(group):0}) 
                if not group in groupList: groupCountsPure.update({str(group):0}) 
                if not group in groupList: groupList.append(group)
            for stream in triggersStreamMap[triggerKey]:
                if not stream in streamList:
                    streamCounts.update({str(stream):0})
                    streamList.append(stream)
            


        #inizialize the number of passed events
        for i in range(len(myPaths)):
            myPassedEvents[myPaths[i]]=0

        #Initialize the correlation matrices
        for dataset1 in primaryDatasetList:
            reducedDatasets.append(dataset1)
            aux_dic = {}
            for dataset2 in primaryDatasetList:
                aux_dic[dataset2] = 0
            datasetDatasetCorrMatrix[dataset1] = aux_dic
            aux_dic={}
            for trigger in myPaths:
                triggerKey = trigger.rstrip("0123456789")
                aux_dic[triggerKey] = 0
            triggerDatasetCorrMatrix[dataset1] = aux_dic


    if isRawFiles:
        # Check condition DST_Physics when processing L1Accept PD
        isDSTPhysics=False
        for triggerName in myPaths:
            if(not "DST_Physics_v" in triggerName): continue
            index = names.triggerIndex(triggerName)
            if checkTriggerIndex(triggerName,index,names.triggerNames()):
                if triggerBits.product().accept(index):
                    isDSTPhysics = True
        if not isDSTPhysics:
            continue


    iPath = 0       

    #here we initialize the counter per dataset to avoid counting a DS twice
    kPassedEvent = False
    datasetsCountsBool = primaryDatasetCounts.fromkeys(primaryDatasetCounts.keys(),False)
    groupCountsBool = groupCounts.fromkeys(groupCounts.keys(),False)
    streamCountsBool = streamCounts.fromkeys(streamCounts.keys(),False)
    triggerCountsBool = {}
    for i in range(0, len(myPaths)):
        triggerCountsBool[myPaths[i]] = False
    myGroupFired = []
    for triggerName in myPaths:
        index = names.triggerIndex(triggerName)
        if checkTriggerIndex(triggerName,index,names.triggerNames()):
            #checking if the event has been accepted by a given trigger
            if triggerBits.product().accept(index):
                myPassedEvents[triggerName]=myPassedEvents[triggerName]+1 
                triggerCountsBool[triggerName] = True
                #we loop over the dictionary keys to see if the paths is in that key, and in case we increase the counter
                triggerKey = triggerName.rstrip("0123456789")
                if triggerKey in datasets.keys():
                    for dataset in datasets[triggerKey]:
                        if datasetsCountsBool[dataset] == False :
                            datasetsCountsBool[dataset] = True
                            primaryDatasetCounts[dataset] = primaryDatasetCounts[dataset] + 1
                    atLeastOneEvent = True
                if triggerKey in groups.keys():
                    for group in groups[triggerKey]:
                        if not physicsStreamOK(triggerKey): continue
                        if group not in myGroupFired: 
                            myGroupFired.append(group)
                            groupCounts[group] = groupCounts[group] + 1
                if triggerKey in triggersStreamMap.keys():
                    for stream in triggersStreamMap[triggerKey]:
                        if streamCountsBool[stream] == False:
                            streamCountsBool[stream] = True
                            streamCounts[stream] += 1

                    



                if kPassedEvent == False:
                    #We only want to count physics streams in the total rate
                    if physicsStreamOK(triggerKey): nPassed_Physics += 1
                    if scoutingStreamOK(triggerKey): nPassed_Scouting += 1
                    kPassedEvent = True

        iPath = iPath+1        
    for dataset1 in primaryDatasetList:
        if not datasetsCountsBool[dataset1]: continue
        for dataset2 in primaryDatasetList:
            if not datasetsCountsBool[dataset2]: continue
            datasetDatasetCorrMatrix[dataset1][dataset2] += 1
        for trigger in myPaths:
            if not triggerCountsBool[trigger]: continue
            triggerKey = trigger.rstrip("0123456789")
            triggerDatasetCorrMatrix[dataset1][triggerKey] += 1

    if len(myGroupFired) == 1:
        groupCountsPure[myGroupFired[0]] = groupCountsPure[myGroupFired[0]] + 1            

    for group in myGroupFired:
        groupCountsShared[group] = groupCountsShared[group] + 1./len(myGroupFired)

        
    n = n+1



#We'll only write the results if there's at least one event
if atLeastOneEvent:

    global_info_file =  open('Results/Raw/Global/output.global'+final_string+'.csv', 'w')
    global_info_file.write("N_LS, " + str(nLS) + "\n")
    global_info_file.write("N_eventsProcessed, " + str(n) + "\n")
    global_info_file.close()
    
    physics_path_file = open('Results/Raw/'+mergeNames['output.path.physics']+'/output.path.physics'+final_string+'.csv', 'w')
    physics_path_file.write("Path, Groups, Counts, Rates (Hz)\n")
    physics_path_file.write("Total Physics, , " + str(nPassed_Physics) + ", " + str(nPassed_Physics) +"\n")
    
    
    
    scouting_path_file = open('Results/Raw/'+mergeNames['output.path.scouting']+'/output.path.scouting'+final_string+'.csv', 'w')
    scouting_path_file.write("Path, Groups, Counts, Rates (Hz)\n")
    scouting_path_file.write("Total Scouting, , " + str(nPassed_Scouting) + ", " + str(nPassed_Scouting) +"\n")
    
    
    
    
    #2d histograms for the correlation matrices
    root_file=ROOT.TFile("Results/Raw/Root/corr_histos"+final_string+".root","RECREATE")
    triggerDataset_histo=ROOT.TH2F("trigger_dataset_corr","Trigger-Dataset Correlations",len(primaryDatasetList),0,len(primaryDatasetList),len(myPaths),0,len(myPaths))
    datasetDataset_histo=ROOT.TH2F("dataset_dataset_corr","Dataset-Dataset Correlations",len(primaryDatasetList),0,len(primaryDatasetList),len(primaryDatasetList),0,len(primaryDatasetList))
    
    triggerDataset_file = open('Results/Raw/'+mergeNames['output.trigger_dataset_corr']+'/output.trigger_dataset_corr'+final_string+'.csv', 'w')
    datasetDataset_file = open('Results/Raw/'+mergeNames['output.dataset_dataset_corr']+'/output.dataset_dataset_corr'+final_string+'.csv', 'w')
    
    
    i = 0
    for dataset in primaryDatasetList:
        i += 1
        triggerDataset_file.write(", " + dataset)
        datasetDataset_file.write(", " + dataset)
    triggerDataset_file.write("\n")
    datasetDataset_file.write("\n")
    
    
    
    
    for i in range(0,len(myPaths)):
        #print myPaths[i], myPassedEvents[i], myPassedEvents[i] 
        trigger = myPaths[i]
        triggerKey = trigger.rstrip("0123456789")
        group_string = ""
        if triggerKey in groups.keys():
            for group in groups[triggerKey]:
                group_string = group_string + group + " "
        if physicsStreamOK(triggerKey):
            physics_path_file.write('{}, {}, {}, {}'.format(trigger, group_string, myPassedEvents[trigger], myPassedEvents[trigger]))
            physics_path_file.write('\n')
        if scoutingStreamOK(triggerKey):
            scouting_path_file.write('{}, {}, {}, {}'.format(trigger, group_string, myPassedEvents[trigger], myPassedEvents[trigger]))
            scouting_path_file.write('\n')
    
    
        triggerDataset_file.write(triggerKey)
        j = 0
        triggerDataset_histo.GetYaxis().SetBinLabel(i+1, triggerKey)
        for dataset in primaryDatasetList:
            if (myPassedEvents[trigger] > 0):
                triggerDatasetCorrMatrix[dataset][triggerKey] = triggerDatasetCorrMatrix[dataset][triggerKey] #/myPassedEvents[trigger]
            triggerDataset_file.write(", " + str(triggerDatasetCorrMatrix[dataset][triggerKey]))
            triggerDataset_histo.GetXaxis().SetBinLabel(j+1, dataset)
            triggerDataset_histo.SetBinContent(j+1, i+1, triggerDatasetCorrMatrix[dataset][triggerKey])
            j += 1
        triggerDataset_file.write("\n")
        
    
    physics_path_file.close()
    scouting_path_file.close()
    triggerDataset_file.close()
    
    physics_dataset_file = open('Results/Raw/'+mergeNames['output.dataset.physics']+'/output.dataset.physics'+final_string+'.csv', 'w')
    scouting_dataset_file = open('Results/Raw/'+mergeNames['output.dataset.scouting']+'/output.dataset.scouting'+final_string+'.csv', 'w')
    
    physics_dataset_file.write("Dataset, Counts, Rates (Hz)\n")
    scouting_dataset_file.write("Dataset, Counts, Rates (Hz)\n")
    i = 0
    for key in primaryDatasetList:
        isPhysicsDataset = False
        isScoutingDataset = False
    
        for trigger in myPaths:
            triggerKey = trigger.rstrip("0123456789")
            if physicsStreamOK(triggerKey) and (key in triggersDatasetMap[triggerKey]): isPhysicsDataset = True
            if scoutingStreamOK(triggerKey) and (key in triggersDatasetMap[triggerKey]): isScoutingDataset = True
        if isPhysicsDataset:
            physics_dataset_file.write(str(key) + ", " + str(primaryDatasetCounts[key]) +", " + str(primaryDatasetCounts[key]))
            physics_dataset_file.write('\n')
        if isScoutingDataset:
            scouting_dataset_file.write(str(key) + ", " + str(primaryDatasetCounts[key]) +", " + str(primaryDatasetCounts[key]))
            scouting_dataset_file.write('\n')
    
        i += 1
        datasetDataset_file.write(key)
        datasetDataset_histo.GetYaxis().SetBinLabel(i, key)
        j = 0
        for key2 in primaryDatasetList:
            j += 1
            if (primaryDatasetCounts[key] > 0): datasetDatasetCorrMatrix[key2][key] = datasetDatasetCorrMatrix[key2][key] #/primaryDatasetCounts[key]
            datasetDataset_file.write(", " + str(datasetDatasetCorrMatrix[key2][key]))
            datasetDataset_histo.GetXaxis().SetBinLabel(j, key2)
            datasetDataset_histo.SetBinContent(j, i, datasetDatasetCorrMatrix[key2][key])
            if i == j : print key2, key
        datasetDataset_file.write("\n")
    
    physics_dataset_file.close()
    scouting_dataset_file.close()
    datasetDataset_file.close()
    
    
    group_file = open('Results/Raw/'+mergeNames['output.group']+'/output.group'+final_string+'.csv','w')
    group_file.write('Groups, Counts, Rates (Hz), Pure Counts, Pure Rates (Hz), Shared Counts, Shared Rates (Hz)\n')
    for key in groupCounts.keys():
        group_file.write(str(key) + ", " + str(groupCounts[key]) +", " + str(groupCounts[key]) + ", " + str(groupCountsPure[key]) +", " + str(groupCountsPure[key]) + ", " + str(groupCountsShared[key]) +", " + str(groupCountsShared[key]))
        group_file.write('\n')
    
    group_file.close()
    
    stream_file = open('Results/Raw/'+mergeNames['output.stream']+'/output.stream'+final_string+'.csv','w')
    stream_file.write('Streams, Counts, Rates (Hz)\n')
    for stream in streamCounts.keys():
        stream_file.write(str(stream) + ", " + str(streamCounts[stream]) +", " + str(streamCounts[stream]) + "\n")
    
    stream_file.close()
    
    #Save histos
    root_file.cd()
    triggerDataset_histo.Write()
    datasetDataset_histo.Write()
    root_file.Close()
