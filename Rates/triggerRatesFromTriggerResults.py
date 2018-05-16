import ROOT
from DataFormats.FWLite import Handle, Events
import math
import json
import sys, getopt
import os


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





triggerList = []
primaryDatasetList = []
primaryDatasetCounts = {}
newDatasetList = []
newDatasetCounts = {}
datasets = {}

groupList = []
groupCounts = {}
groupCountsShared = {}
groupCountsPure = {}
groups = {}

streamList = []
streamCounts = {}

metBx = ROOT.TH1F("metBx","",4000,0.,4000.)
muonBx = ROOT.TH1F("muonBx","",4000,0.,4000.)
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
parser.add_option("-s","--finalstring",dest="finalString",type="str",default="nostr",help="STRING used to name the output",metavar="STRING")
parser.add_option("-f","--filetype",dest="fileType",type="str",default="custom",help="ARG='custom' (default option), 'RAW' or 'L1Accept', use 'custom' if you're running on STEAM-made files, 'RAW' if you're running on raw data, 'L1Accept' if you're running on L1Accept data",metavar="ARG")
parser.add_option("-m","--maps",dest="maps",type="str",default="nomaps",help="ARG='nomaps' (default option, don't use maps to get dataste/groups/etc. rates), 'somemaps' (get dataste/groups/etc. rates but with no study of dataset merging), 'allmaps' (get dataste/groups/etc. rates and also study dataset merging)",metavar="ARG")

opts, args = parser.parse_args()


error_text = '\nError: wrong inputs\n'
help_text = '\npython triggerRatesFromTriggerResults.py -i <inputfile> -j <json> -s <finalstring> -f <filetype> -m <maps>'
help_text += '\n<inputfile> (mandatory argument) = one (1) input root file'
help_text += '\n<json> (mandatory) = text file with the LS range in json format'
help_text += '\n<finalstring> (mandatory) = string which will provide a unique tag to the output'
help_text += '\n<filetype> (optional) = "custom" (default option) or "RAW" or "L1Accept"'
help_text += '\n<maps> (optional) = "nomaps" (default option, use none of the maps), "somemaps" (use all maps except those related to dataset merging), "allmaps" (use all maps, including dataset merging)\n'

if opts.inputFile == "noroot" or opts.jsonFile == "nojson" or opts.finalString == "nostr":
    print error_text
    print help_text
    sys.exit(2)



isRawFiles = False
isL1Accept = False
if opts.fileType == "custom":
    isRawFiles = False
elif opts.fileType == "RAW":
    isRawFiles = True
elif opts.fileType == "L1Accept":
    isRawFiles = True
    isL1Accept = True
else:
    print "<filetype> input '%s' is wrong" %opts.fileType
    print error_text
    print help_text
    sys.exit(2)

final_string = opts.finalString

bUseMaps = False
if opts.maps == "allmaps":
    bUseMaps = True
    from Menu_HLT import newDatasetMap
    from Menu_HLT import groupMap as triggersGroupMap
    from Menu_HLT import datasetMap as  triggersDatasetMap
    from Menu_HLT import streamMap as  triggersStreamMap
    from aux import physicsStreamOK
    from aux import scoutingStreamOK
    from aux import parkingStreamOK

elif opts.maps == "somemaps":
    bUseMaps = True
    from Menu_HLT import groupMap as triggersGroupMap
    from Menu_HLT import datasetMap as  triggersDatasetMap
    from Menu_HLT import streamMap as  triggersStreamMap
    from aux import physicsStreamOK
    from aux import scoutingStreamOK
    from aux import parkingStreamOK

elif opts.maps == "nomaps":
    bUseMaps = False
else:
    print "<maps> input '%s' is wrong" %opts.maps
    print error_text
    print help_text
    sys.exit(2)
    

#Handles and labels
if isRawFiles:
    triggerBits, triggerBitLabel = Handle("edm::TriggerResults"), ("TriggerResults::HLT")
else:
    triggerBits, triggerBitLabel = Handle("edm::TriggerResults"), ("TriggerResults::MYHLT")


#Looping over the inputfiles
n = -1
nPassed_Physics = 0
nPassed_Scouting = 0
nPassed_Parking = 0
nPassed_Misc = 0

#List of triggers
myPaths = []
myPassedEvents = {}

nLS = 0


triggerDatasetCorrMatrix = {}
datasetDatasetCorrMatrix = {}
if opts.maps == "allmaps":
    triggerNewDatasetCorrMatrix = {}
    newDatasetNewDatasetCorrMatrix = {}



#get rates from input file
events = Events (opts.inputFile)

#Looping over events in inputfile

runAndLsList = []
atLeastOneEvent = False
nEvents = 0
save=0
s_triggerKey=""
s_dataset1=""
for event in events: 
    n += 1
    #if n == 10000: break
    #taking trigger informations: names, bits and products
    event.getByLabel(triggerBitLabel, triggerBits)
    names = event.object().triggerNames(triggerBits.product())    


    #initializing stuff
    if n<1:
        for name in names.triggerNames():
            name = str(name)
            if ("HLTriggerFirstPath" in name) or ("HLTriggerFinalPath" in name): continue
            myPaths.append(name)
            if bUseMaps:
                triggerKey = name.rstrip("0123456789")
                if not triggerKey in triggersDatasetMap: continue
                datasets.update({str(triggerKey):triggersDatasetMap[triggerKey]})
                groups.update({str(triggerKey):triggersGroupMap[triggerKey]})
                if not (name in triggerList) :triggerList.append(name)
                for dataset in triggersDatasetMap[triggerKey]:
                    if not dataset in primaryDatasetList: primaryDatasetCounts.update({str(dataset):0}) 
                    if not dataset in primaryDatasetList: primaryDatasetList.append(dataset)
                    if opts.maps == "allmaps":
                        newDataset = dataset
                        if dataset in newDatasetMap.keys():
                            newDataset = newDatasetMap[dataset]
                        if newDataset not in newDatasetList:
                            newDatasetCounts.update({str(newDataset):0})
                            newDatasetList.append(newDataset)
                for group in triggersGroupMap[triggerKey]:
                    if not group in groupList: groupCounts.update({str(group):0}) 
                    if not group in groupList: groupCountsShared.update({str(group):0}) 
                    if not group in groupList: groupCountsPure.update({str(group):0}) 
                    if not group in groupList: groupList.append(group)
                for stream in triggersStreamMap[triggerKey]:
                    if not stream in streamList:
                        streamCounts.update({str(stream):0})
                        streamList.append(stream)
        #print primaryDatasetList
        #inizialize the number of passed events
        for i in range(len(myPaths)):
            myPassedEvents[myPaths[i]]=0

        if bUseMaps:
            #Initialize the correlation matrices
            dummy_nonpure = "NonPure"
            aux_dic = {}
            for dataset1 in primaryDatasetList:
                aux_dic = {}
                for dataset2 in primaryDatasetList:
                    aux_dic[dataset2] = 0
                datasetDatasetCorrMatrix[dataset1] = aux_dic.copy()
                aux_dic={}
                for trigger in myPaths:
                    triggerKey = trigger.rstrip("0123456789")
                    aux_dic[triggerKey] = 0
                triggerDatasetCorrMatrix[dataset1] = aux_dic.copy()
            triggerDatasetCorrMatrix[dummy_nonpure] = aux_dic.copy()
            
            if opts.maps == "allmaps":
                for dataset1 in newDatasetList:
                    aux_dic = {}
                    for dataset2 in newDatasetList:
                        aux_dic[dataset2] = 0
                    newDatasetNewDatasetCorrMatrix[dataset1] = aux_dic.copy()
                    aux_dic={}
                    for trigger in myPaths:
                        triggerKey = trigger.rstrip("0123456789")
                        aux_dic[triggerKey] = 0
                    triggerNewDatasetCorrMatrix[dataset1] = aux_dic.copy()
                triggerNewDatasetCorrMatrix[dummy_nonpure] = aux_dic.copy()


    #check if event is in the json range
    runnbr = event.object().id().run()
    runls = event.object().id().luminosityBlock()
    runstr = str((runnbr,runls))
    if not check_json(opts.jsonFile, runnbr, runls):
        continue
    if not runstr in runAndLsList:
        nLS = nLS +1
        runAndLsList.append(runstr)


    if isL1Accept:
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
    kPassedEventPhysics = False
    kPassedEventScouting = False
    kPassedEventParking = False
    kPassedEventMisc = False
    triggerCountsBool = {}
    for i in range(0, len(myPaths)):
        triggerCountsBool[myPaths[i]] = False
    if bUseMaps:
        datasetsLatestCounts = primaryDatasetCounts.fromkeys(primaryDatasetCounts.keys(),0)
        if opts.maps == "allmaps":
            newDatasetsLatestCounts = newDatasetCounts.fromkeys(newDatasetCounts.keys(),0)
        groupCountsBool = groupCounts.fromkeys(groupCounts.keys(),False)
        streamCountsBool = streamCounts.fromkeys(streamCounts.keys(),False)
        myGroupFired = []
    for triggerName in myPaths:
        index = names.triggerIndex(triggerName)
        if checkTriggerIndex(triggerName,index,names.triggerNames()):
            #checking if the event has been accepted by a given trigger
            if triggerBits.product().accept(index):
                if "HLT_PFMET" in str(triggerName):
                    metBx.Fill(event.object().bunchCrossing()*1.)

                if "HLT_IsoMu24" in str(triggerName):
                    muonBx.Fill(event.object().bunchCrossing()*1.)

                myPassedEvents[triggerName]=myPassedEvents[triggerName]+1 
                atLeastOneEvent = True
                triggerCountsBool[triggerName] = True
                if not bUseMaps:
                    if not kPassedEventMisc:
                        kPassedEventMisc = True
                        nPassed_Misc += 1
                else:
                    #we loop over the dictionary keys to see if the paths is in that key, and in case we increase the counter
                    triggerKey = triggerName.rstrip("0123456789")
                    if triggerKey in datasets.keys():
                        for dataset in datasets[triggerKey]:
                            if datasetsLatestCounts[dataset] == 0 :
                                primaryDatasetCounts[dataset] = primaryDatasetCounts[dataset] + 1
                            datasetsLatestCounts[dataset] += 1
                            if opts.maps == "allmaps":
                                newDataset = dataset
                                if dataset in newDatasetMap.keys():
                                    newDataset = newDatasetMap[dataset]
                                if newDatasetsLatestCounts[newDataset] == 0 :
                                    newDatasetCounts[newDataset] += 1
                                newDatasetsLatestCounts[newDataset] += 1
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
                    
                    
                    if kPassedEventPhysics == False:
                        if physicsStreamOK(triggerKey):
                            nPassed_Physics += 1
                            kPassedEventPhysics = True
                    
                    if kPassedEventScouting == False:
                        if scoutingStreamOK(triggerKey):
                            nPassed_Scouting += 1
                            kPassedEventScouting = True

                    if kPassedEventParking == False:
                        if parkingStreamOK(triggerKey):
                            nPassed_Parking += 1
                            kPassedEventParking = True

                    if kPassedEventMisc == False:
                        if not (parkingStreamOK(triggerKey) or scoutingStreamOK(triggerKey) or physicsStreamOK(triggerKey)):
                            nPassed_Misc += 1
                            kPassedEventMisc = True

        iPath = iPath+1
    if bUseMaps:
        #if (n> 6584 and n<=7318) or (n==7569) or n==6209: print n,triggerDatasetCorrMatrix[dummy_nonpure]["HLT_IsoMu27_v"]
        for dataset1 in primaryDatasetList:
            if datasetsLatestCounts[dataset1] == 0: continue
            for dataset2 in primaryDatasetList:
                if datasetsLatestCounts[dataset2] == 0: continue
                datasetDatasetCorrMatrix[dataset1][dataset2] += 1
            for trigger in myPaths:
                if not triggerCountsBool[trigger]: continue
                triggerKey = trigger.rstrip("0123456789")
                triggerDatasetCorrMatrix[dataset1][triggerKey] += 1 #somehow this is increasing the non-pure rate when dataset1 = unassigned
                if triggerKey in datasets.keys():
                    if (dataset1 in triggersDatasetMap[triggerKey]) and datasetsLatestCounts[dataset1] > 1:
                        triggerDatasetCorrMatrix[dummy_nonpure][triggerKey] += 1
        if opts.maps == "allmaps":
            for dataset1 in newDatasetList:
                if newDatasetsLatestCounts[dataset1] == 0: continue
                for dataset2 in newDatasetList:
                    if newDatasetsLatestCounts[dataset2] == 0: continue
                    newDatasetNewDatasetCorrMatrix[dataset1][dataset2] += 1
                for trigger in myPaths:
                    if not triggerCountsBool[trigger]: continue
                    triggerKey = trigger.rstrip("0123456789")
                    triggerNewDatasetCorrMatrix[dataset1][triggerKey] += 1
                    bUseDummy = False
                    if triggerKey in datasets.keys():
                        if newDatasetsLatestCounts[dataset1] > 1:
                            if dataset1 in triggersDatasetMap[triggerKey]:
                                bUseDummy = True
                            elif not (dataset1 in primaryDatasetList):
                                for old_dataset in newDatasetMap.keys():
                                    if not (dataset1 in newDatasetMap[old_dataset]): continue
                                    if old_dataset in triggersDatasetMap[triggerKey]:
                                        bUseDummy = True
                                        break
                    if bUseDummy: triggerNewDatasetCorrMatrix[dummy_nonpure][triggerKey] += 1
        
        if len(myGroupFired) == 1:
            groupCountsPure[myGroupFired[0]] = groupCountsPure[myGroupFired[0]] + 1            
        
        for group in myGroupFired:
            groupCountsShared[group] = groupCountsShared[group] + 1./len(myGroupFired)

    nEvents += 1

n += 1
#We'll only write the results if there's at least one event
if atLeastOneEvent:

    global_info_file =  open('Results/Raw/Global/output.global'+final_string+'.csv', 'w')
    global_info_file.write("N_LS, " + str(nLS) + "\n")
    global_info_file.write("N_eventsInLoop, " + str(n) + "\n")
    global_info_file.write("N_eventsProcessed, " + str(nEvents) + "\n")
    global_info_file.close()
    
    misc_path_file = open('Results/Raw/'+mergeNames['output.path.misc']+'/output.path.misc'+final_string+'.csv', 'w')
    misc_path_file.write("Path, Groups, Counts, Rates (Hz)\n")
    misc_path_file.write("Total Misc, , " + str(nPassed_Misc) + ", " + str(nPassed_Misc) +"\n")


    root_file=ROOT.TFile("Results/Raw/Root/histos"+final_string+".root","RECREATE")
    if bUseMaps:
        physics_path_file = open('Results/Raw/'+mergeNames['output.path.physics']+'/output.path.physics'+final_string+'.csv', 'w')
        physics_path_file.write("Path, Groups, Counts, Rates (Hz)\n")
        physics_path_file.write("Total Physics, , " + str(nPassed_Physics) + ", " + str(nPassed_Physics) +"\n")
        
        scouting_path_file = open('Results/Raw/'+mergeNames['output.path.scouting']+'/output.path.scouting'+final_string+'.csv', 'w')
        scouting_path_file.write("Path, Groups, Counts, Rates (Hz)\n")
        scouting_path_file.write("Total Scouting, , " + str(nPassed_Scouting) + ", " + str(nPassed_Scouting) +"\n")
        
        parking_path_file = open('Results/Raw/'+mergeNames['output.path.parking']+'/output.path.parking'+final_string+'.csv', 'w')
        parking_path_file.write("Path, Groups, Counts, Rates (Hz)\n")
        parking_path_file.write("Total Parking, , " + str(nPassed_Parking) + ", " + str(nPassed_Parking) +"\n")
        
        
        
        
        #2d histograms for the correlation matrices
        triggerDataset_histo=ROOT.TH2F("trigger_dataset_corr","Trigger-Dataset Correlations",len(primaryDatasetList)+1,0,len(primaryDatasetList)+1,len(myPaths),0,len(myPaths))
        datasetDataset_histo=ROOT.TH2F("dataset_dataset_corr","Dataset-Dataset Correlations",len(primaryDatasetList),0,len(primaryDatasetList),len(primaryDatasetList),0,len(primaryDatasetList))
        
        if opts.maps == "allmaps":
            triggerNewDataset_histo=ROOT.TH2F("trigger_newDataset_corr","New Trigger-Dataset Correlations",len(newDatasetList)+1,0,len(newDatasetList)+1,len(myPaths),0,len(myPaths))
            newDatasetNewDataset_histo=ROOT.TH2F("newDataset_newDataset_corr","New Dataset-Dataset Correlations",len(newDatasetList),0,len(newDatasetList),len(newDatasetList),0,len(newDatasetList))
            triggerNewDataset_file = open('Results/Raw/'+mergeNames['output.trigger_newDataset_corr']+'/output.trigger_newDataset_corr'+final_string+'.csv', 'w')
            newDatasetNewDataset_file = open('Results/Raw/'+mergeNames['output.newDataset_newDataset_corr']+'/output.newDataset_newDataset_corr'+final_string+'.csv', 'w')
        
        triggerDataset_file = open('Results/Raw/'+mergeNames['output.trigger_dataset_corr']+'/output.trigger_dataset_corr'+final_string+'.csv', 'w')
        datasetDataset_file = open('Results/Raw/'+mergeNames['output.dataset_dataset_corr']+'/output.dataset_dataset_corr'+final_string+'.csv', 'w')
        
        
        
        i = 0
        for dataset in primaryDatasetList:
            i += 1
            triggerDataset_file.write(", " + dataset)
            datasetDataset_file.write(", " + dataset)
        triggerDataset_file.write(", "+dummy_nonpure+"\n")
        datasetDataset_file.write("\n")
        
        if opts.maps == "allmaps":
            for dataset in newDatasetList:
                triggerNewDataset_file.write(", " + dataset)
                newDatasetNewDataset_file.write(", " + dataset)
            triggerNewDataset_file.write(", "+dummy_nonpure+"\n")
            newDatasetNewDataset_file.write("\n")
    
    
    
    for i in range(0,len(myPaths)):
        #print myPaths[i], myPassedEvents[i], myPassedEvents[i] 
        trigger = myPaths[i]
        triggerKey = trigger.rstrip("0123456789")
        group_string = ""
        if not bUseMaps:
            misc_path_file.write('{}, {}, {}, {}'.format(trigger, group_string, myPassedEvents[trigger], myPassedEvents[trigger]))
            misc_path_file.write('\n')
        else:
            if triggerKey in groups.keys():
                for group in groups[triggerKey]:
                    group_string = group_string + group + " "
            if physicsStreamOK(triggerKey):
                physics_path_file.write('{}, {}, {}, {}'.format(trigger, group_string, myPassedEvents[trigger], myPassedEvents[trigger]))
                physics_path_file.write('\n')
            if scoutingStreamOK(triggerKey):
                scouting_path_file.write('{}, {}, {}, {}'.format(trigger, group_string, myPassedEvents[trigger], myPassedEvents[trigger]))
                scouting_path_file.write('\n')
            if parkingStreamOK(triggerKey):
                parking_path_file.write('{}, {}, {}, {}'.format(trigger, group_string, myPassedEvents[trigger], myPassedEvents[trigger]))
                parking_path_file.write('\n')
            if not (parkingStreamOK(triggerKey) or scoutingStreamOK(triggerKey) or physicsStreamOK(triggerKey)):
                misc_path_file.write('{}, {}, {}, {}'.format(trigger, group_string, myPassedEvents[trigger], myPassedEvents[trigger]))
                misc_path_file.write('\n')

            triggerDataset_file.write(triggerKey)
            j = 0
            triggerDataset_histo.GetYaxis().SetBinLabel(i+1, triggerKey)
            for dataset in primaryDatasetList:
                triggerDataset_file.write(", " + str(triggerDatasetCorrMatrix[dataset][triggerKey]))
                triggerDataset_histo.GetXaxis().SetBinLabel(j+1, dataset)
                triggerDataset_histo.SetBinContent(j+1, i+1, triggerDatasetCorrMatrix[dataset][triggerKey])
                j += 1
            triggerDataset_histo.GetXaxis().SetBinLabel(j+1, dummy_nonpure)
            triggerDataset_histo.SetBinContent(j+1, i+1, triggerDatasetCorrMatrix[dummy_nonpure][triggerKey])
            triggerDataset_file.write(", " + str(triggerDatasetCorrMatrix[dummy_nonpure][triggerKey]))
            triggerDataset_file.write("\n")
            
            if opts.maps == "allmaps":
                triggerNewDataset_file.write(triggerKey)
                j = 0
                triggerNewDataset_histo.GetYaxis().SetBinLabel(i+1, triggerKey)
                for dataset in newDatasetList:
                    triggerNewDataset_file.write(", " + str(triggerNewDatasetCorrMatrix[dataset][triggerKey]))
                    triggerNewDataset_histo.GetXaxis().SetBinLabel(j+1, dataset)
                    triggerNewDataset_histo.SetBinContent(j+1, i+1, triggerNewDatasetCorrMatrix[dataset][triggerKey])
                    j += 1
                triggerNewDataset_histo.GetXaxis().SetBinLabel(j+1, dummy_nonpure)
                triggerNewDataset_histo.SetBinContent(j+1, i+1, triggerNewDatasetCorrMatrix[dummy_nonpure][triggerKey])
                triggerNewDataset_file.write(", " + str(triggerNewDatasetCorrMatrix[dummy_nonpure][triggerKey]))
                triggerNewDataset_file.write("\n")
        
    
    misc_path_file.close()
    if bUseMaps:
        physics_path_file.close()
        scouting_path_file.close()
        triggerDataset_file.close()
        if opts.maps == "allmaps":
            triggerNewDataset_file.close()
    
        physics_dataset_file = open('Results/Raw/'+mergeNames['output.dataset.physics']+'/output.dataset.physics'+final_string+'.csv', 'w')
        misc_dataset_file = open('Results/Raw/'+mergeNames['output.dataset.misc']+'/output.dataset.misc'+final_string+'.csv', 'w')
        
        physics_dataset_file.write("Dataset, Counts, Rates (Hz)\n")
        misc_dataset_file.write("Dataset, Counts, Rates (Hz)\n")
        i = 0
        for key in primaryDatasetList:
            isPhysicsDataset = False
        
            for trigger in myPaths:
                triggerKey = trigger.rstrip("0123456789")
                if physicsStreamOK(triggerKey) and (key in triggersDatasetMap[triggerKey]): isPhysicsDataset = True
            if isPhysicsDataset:
                physics_dataset_file.write(str(key) + ", " + str(primaryDatasetCounts[key]) +", " + str(primaryDatasetCounts[key]))
                physics_dataset_file.write('\n')
            else:
                misc_dataset_file.write(str(key) + ", " + str(primaryDatasetCounts[key]) +", " + str(primaryDatasetCounts[key]))
                misc_dataset_file.write('\n')
        
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
        
        if opts.maps == "allmaps":
            newDataset_file = open('Results/Raw/'+mergeNames['output.newDataset.physics']+'/output.newDataset.physics'+final_string+'.csv', 'w')
            newDataset_file.write("Dataset, Counts, Rates (Hz)\n")
            i = 0
            for key in newDatasetList:
                isPhysicsDataset = False
            
                for trigger in myPaths:
                    triggerKey = trigger.rstrip("0123456789")
                    if physicsStreamOK(triggerKey):
                        if (key in triggersDatasetMap[triggerKey]):
                            isPhysicsDataset = True
                        elif not (key in primaryDatasetList):
                            for old_dataset in newDatasetMap.keys():
                                if not (key in newDatasetMap[old_dataset]): continue
                                if old_dataset in triggersDatasetMap[triggerKey]:
                                    isPhysicsDataset = True
                                    break
                if isPhysicsDataset:
                    newDataset_file.write(str(key) + ", " + str(newDatasetCounts[key]) +", " + str(newDatasetCounts[key]))
                    newDataset_file.write('\n')
            
                i += 1
                newDatasetNewDataset_file.write(key)
                newDatasetNewDataset_histo.GetYaxis().SetBinLabel(i, key)
                j = 0
                for key2 in newDatasetList:
                    j += 1
                    newDatasetNewDataset_file.write(", " + str(newDatasetNewDatasetCorrMatrix[key2][key]))
                    newDatasetNewDataset_histo.GetXaxis().SetBinLabel(j, key2)
                    newDatasetNewDataset_histo.SetBinContent(j, i, newDatasetNewDatasetCorrMatrix[key2][key])
                newDatasetNewDataset_file.write("\n")
            
            newDataset_file.close()
            newDatasetNewDataset_file.close()
        
        
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
    if bUseMaps:
        triggerDataset_histo.Write()
        datasetDataset_histo.Write()
        if opts.maps == "allmaps":
            triggerNewDataset_histo.Write()
            newDatasetNewDataset_histo.Write()
    metBx.Write()
    muonBx.Write()
    root_file.Close()
