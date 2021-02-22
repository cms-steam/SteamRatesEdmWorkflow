import ROOT
from DataFormats.FWLite import Handle, Events
import math
import json
import sys, getopt
import os


#Start auxiliary functions

#Function to check if a given trigger path name is present in the input ROOT file
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

#Function to check if a given (run number, lumi section) combination is accepted by the JSON file
#Returns True when accepted, False when not
def check_json(jsonf, runNo_in, LS, bMC):
    if bMC:  #if running on MC
        return True  #No JSON file used when running on MC, so the check always succeeds
    runNo = str(runNo_in)
    file1=open(jsonf,'r')
    inp1={}
    text = ""
    for line1 in file1:
        text+=line1
    inp1 = json.loads(text)  #load JSON text as a python map
    #print inp1.keys()
    if runNo in inp1:
        for part_LS in inp1[runNo]:
            if LS >= part_LS[0] and LS <= part_LS[1]:
                #part_LS[0]: lower LS boundary, part_LS[1]: upper LS boundary
                return True
    return False



#Declaration of various lists and maps which will be filled later
triggerList = []          #obsolete, should be removed
primaryDatasetList = []   #list of primary datasets (if using "somemaps" or "allmaps" options)
primaryDatasetCounts = {} #event count for each dataset (stored in a map)
newDatasetList = []       #list of datasets after some are merged (if using "allmaps" option)
newDatasetCounts = {}     #event counts for the new datasets
datasets = {}             #trigger->dataset map

groupList = []            #auxiliary list used to avoid including the same group twice when initializing group counts
groupCounts = {}          #event count for each group
groupCountsShared = {}    #"shared" event count for each group (when an event is shared by N groups, it's weighted 1/N)
groupCountsPure = {}      #"pure" event count for each group (excludes events shared by many groups)
groups = {}               #trigger->group map

streamList = []           #auxiliary list, similar to the one for groups
streamCounts = {}         #counts for each stream
streams = {}              #trigger->stream map

types = {}                #trigger->type (control, signal, backup, etc.) map


#Obsolete, ignore
metBx = ROOT.TH1F("metBx","",4000,0.,4000.)
muonBx = ROOT.TH1F("muonBx","",4000,0.,4000.)
#End obsolete



#get inputs
from optparse import OptionParser
parser=OptionParser()
parser.add_option("-i","--infile",dest="inputFile",type="str",default="noroot",help="one (1) input root FILE",metavar="FILE")
parser.add_option("-j","--json",dest="jsonFile",type="str",default="nojson",help="If running over data: text file with the LS range in json format. If running over MC: name of the MC dataset.")
parser.add_option("-s","--finalstring",dest="finalString",type="str",default="nostr",help="STRING used to name the output",metavar="STRING")
parser.add_option("-f","--filetype",dest="fileType",type="str",default="custom",help="ARG='custom' (default option), 'RAW' or 'L1Accept', use 'custom' if you're running on STEAM-made files, 'RAW' if you're running on raw data, 'L1Accept' if you're running on L1Accept data",metavar="ARG")
parser.add_option("-m","--maps",dest="maps",type="str",default="nomaps",help="ARG='nomaps' (default option, don't use maps to get dataste/groups/etc. rates), 'somemaps' (get dataste/groups/etc. rates but with no study of dataset merging), 'allmaps' (get dataste/groups/etc. rates and also study dataset merging)",metavar="ARG")
parser.add_option("-M","--maxEvents",dest="maxEvents",type="int",default=-1,help="maximum number of events to be processed (default -1 to process all events)",metavar="INT")

opts, args = parser.parse_args()


error_text = '\nError: wrong inputs\n'
help_text = '\npython triggerCountsFromTriggerResults.py -i <inputfile> -j <json/dataset> -s <finalstring> -f <filetype> -m <maps> -M <maxEvents>'
help_text += '\n<inputfile> (mandatory argument) = one (1) input root file'
help_text += '\n<json/dataset> (mandatory) = If running over data: text file with the LS range in json format. If running over MC: name of the MC dataset.'
help_text += '\n<finalstring> (mandatory) = string which will provide a unique tag to the output'
help_text += '\n<filetype> (optional) = "custom" (default option) or "RAW" or "L1Accept"'
help_text += '\n<maps> (optional) = "nomaps" (default option, use none of the maps), "somemaps" (use all maps except those related to dataset merging) or "allmaps" (use all maps, including dataset merging)\n'
help_text += '\n<maxEvents> (optional) maximum number of events to be processed\n'

if opts.inputFile == "noroot" or opts.finalString == "nostr" or opts.jsonFile == "nojson":
    #These are the "mandatory options" so print an error if thay weren't specified
    print error_text
    print help_text
    sys.exit(2)


#booleans representing filetype information
isRawFiles = False
isL1Accept = False
#boolean for data/MC
isMC = False
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

MCdataset=""
#If this is a data job, the JSON input should be a file ending in ".txt" or ".json"
#If not the code assumes that this is a MC job
#So the code differentiates between data and MC jobs by looking at the JSON input
if not (".txt" in opts.jsonFile or ".json" in opts.jsonFile):
    #If the end of the file isn't what's expected for a JSON
    #We assume it's an MC job and the JSON input is in fact the name of the MC dataset
    print "\nNonsense JSON provided, assuming this is a MC job...\n"
    MCdataset=opts.jsonFile
    isRawFiles = False
    isL1Accept = False
    isMC = True
else:
    #check existence of JSON file
    try:
        os.system("ls %s"%opts.jsonFile)
    except:
        print "\n\n\n!!! JSON file not found!!!\n\n\n"
        sys.exit(2)


final_string = opts.finalString  #retrieve tag which is added to the end of the name of all output files
maxEvents = opts.maxEvents       #retrieve max number of events

bUseMaps = False   #booleans indicating whether maps from "Menu_HLT.py" are used
if opts.maps == "allmaps":
    bUseMaps = True
    #import map describing dataset merging
    from Menu_HLT import newDatasetMap    
    #import other maps
    from Menu_HLT import groupMap as triggersGroupMap
    from Menu_HLT import datasetMap as  triggersDatasetMap
    from Menu_HLT import streamMap as  triggersStreamMap
    from Menu_HLT import typeMap as  triggersTypeMap

    #import functions that check if a path belongs to a physics, scouting or parking stream, and if the path belongs to a PAG
    from aux import physicsStreamOK
    from aux import scoutingStreamOK
    from aux import parkingStreamOK
    from aux import belongsToPAG

elif opts.maps == "somemaps":
    bUseMaps = True
    from Menu_HLT import groupMap as triggersGroupMap
    from Menu_HLT import datasetMap as  triggersDatasetMap
    from Menu_HLT import streamMap as  triggersStreamMap
    from Menu_HLT import typeMap as  triggersTypeMap
    from aux import physicsStreamOK
    from aux import scoutingStreamOK
    from aux import parkingStreamOK
    from aux import belongsToPAG

elif opts.maps == "nomaps":
    bUseMaps = False
else:
    #The maps option can only take 1 of 3 values, raise an error otherwise
    print "<maps> input '%s' is wrong" %opts.maps
    print error_text
    print help_text
    sys.exit(2)
    

#Handles and labels
#Note the slightly different name of the TriggerResults::HLT collection when using STEAM root files.
if isRawFiles:
    triggerBits, triggerBitLabel = Handle("edm::TriggerResults"), ("TriggerResults::HLT")
else:
    triggerBits, triggerBitLabel = Handle("edm::TriggerResults"), ("TriggerResults::MYHLT")


#Looping over the inputfiles
nLoop = -1            #counter for number of events processed (including those rejected by JSON)
nPassed_Physics = 0   #counter for events passing "physics" trigger paths (if using "somemaps" or "allmaps" options)
nPassed_Scouting = 0  #counter for scouting triggers
nPassed_Parking = 0   #counter for parking triggers
nPassed_Misc = 0      #counter for all other triggers (if using "somemaps" or "allmaps" options, if using "nomaps" this counts all events passing any trigger)

myPaths = []          #list of trigger path names in the input ROOT file
myPassedEvents = {}   #event counter for each trigger path (+separate counter for "pure" events)

nLS = 0                #counter for lumi sections processed in the input root file which pass the JSON selection
nPAGAnalysisPath = 0   #counter of events useful for "physics" analyses

#trigger-dataset and dataset-dataset correlation matrices
triggerDatasetCorrMatrix = {}
datasetDatasetCorrMatrix = {}
if opts.maps == "allmaps":
    #correlation matrices for the post-merging datasets
    triggerNewDatasetCorrMatrix = {}
    newDatasetNewDatasetCorrMatrix = {}


print "before loading input"
#get rates from input file
events = Events (opts.inputFile)
print "after loading input"

#Looping over events in inputfile

default_name = ["unknown"]  #default name for datasets/groups/streams
runAndLsList = []           #list of (run, LS) processed in this files, to avoid double counting when incrementing nLS
atLeastOneEvent = False
nEvents = 0                 #counter of events passing JSON
for event in events: 
    nLoop += 1      #increment loop counter

    if nLoop%1000==0:
        print "Processing entry ",nLoop

    if maxEvents>0 and nLoop >= maxEvents: 
        break

    #taking trigger informations: names, bits and products
    event.getByLabel(triggerBitLabel, triggerBits)
    names = event.object().triggerNames(triggerBits.product())    


    #initializing stuff
    if nLoop<1:
        for name in names.triggerNames():   #loop over trigger path names
            name = str(name)
            strippedTrigger = name.rstrip("0123456789")   #strip the version number at the end of the trigger name
            #if strippedTrigger in triggersToIgnore: continue
            if ("HLTriggerFirstPath" in name) or ("HLTriggerFinalPath" in name): continue
            myPaths.append(name)     #list of all trigger path names in the file
            if bUseMaps:
                bVersionNumbers = True  #boolean to determine if the trigger paths in "Menu_HLT.py" have version numbers at the end or not
                for key in triggersDatasetMap.keys():
                    if key.rstrip("0123456789") == strippedTrigger:
                        #trigger paths end with "_vN" where N is the version number
                        #A trigger path ending in "v" had its version number stripped already
                        if key.endswith("v"): bVersionNumbers = False 
                        break
                actualKey = ""
                if bVersionNumbers:
                    actualKey = name
                else:
                    actualKey = strippedTrigger
                #the key you need to you use for the map imported from "Menu_HLT.py" depends on whether the version number was stripped or not

                #check if the trigger path in this iteration of the loop is a key in the maps imported from "Menu_HLT.py"
                datasetKnown = False
                if actualKey in triggersDatasetMap:   
                    datasets.update({str(strippedTrigger):triggersDatasetMap[actualKey]})
                    datasetKnown = True
                else:
                    datasets.update({str(strippedTrigger):default_name})

                if actualKey in triggersGroupMap:
                    groups.update({str(strippedTrigger):triggersGroupMap[actualKey]})
                else:
                    groups.update({str(strippedTrigger):default_name})
                    if datasetKnown:
                        print "group UNKNOWN while dataset is known"  
                        print strippedTrigger

                if actualKey in triggersStreamMap:
                    streams.update({str(strippedTrigger):triggersStreamMap[actualKey]})
                else:
                    streams.update({str(strippedTrigger):default_name})

                if actualKey in triggersTypeMap:
                    types.update({str(strippedTrigger):triggersTypeMap[actualKey]})
                else:
                    types.update({str(strippedTrigger):default_name})
                    
                if not (name in triggerList) :triggerList.append(name)  #obsolete line
                for dataset in datasets[strippedTrigger]:
                    if not dataset in primaryDatasetList: primaryDatasetCounts.update({str(dataset):0}) #initialise dataset counts
                    if not dataset in primaryDatasetList: primaryDatasetList.append(dataset)            #avoid including the same dataset twice
                    if opts.maps == "allmaps": #when using the option to study dataset merging
                        newDataset = dataset   #by default, the new dataset is the same as the old one
                        if dataset in newDatasetMap.keys():
                            newDataset = newDatasetMap[dataset]  #but if the new dataset map says it's different, then change it
                        if newDataset not in newDatasetList:
                            newDatasetCounts.update({str(newDataset):0})  #initialise dataset counts for the "new" datasets (after merging)
                            newDatasetList.append(newDataset)
                for group in groups[strippedTrigger]:
                    #initialise group counts
                    if not group in groupList: groupCounts.update({str(group):0}) 
                    if not group in groupList: groupCountsShared.update({str(group):0}) 
                    if not group in groupList: groupCountsPure.update({str(group):0}) 
                    #avoid including the same group twice
                    if not group in groupList: groupList.append(group)
                for stream in streams[strippedTrigger]:
                    if not stream in streamList:   #initialize stream counts
                        streamCounts.update({str(stream):0})
                        streamList.append(stream)

        #inizialize the number of passed events
        for i in range(len(myPaths)):
            myPassedEvents[myPaths[i]]=[0,0] #map trigger path -> [total count, pure count]

        if bUseMaps:
            #Initialize the correlation matrices
            dummy_nonpure = "NonPure"  #useful to tag non-pure trigger-dataset correlation
            aux_dic = {}
            for dataset1 in primaryDatasetList:
                aux_dic = {}
                for dataset2 in primaryDatasetList:
                    aux_dic[dataset2] = 0  #initialise dataset-dataset overlap counters to zero
                datasetDatasetCorrMatrix[dataset1] = aux_dic.copy()  #datasetDatasetCorrMatrix is a map of maps, hence the need for an auxiliary map (maps are also known as dictionaries)
                aux_dic={}
                for trigger in myPaths:
                    strippedTrigger = trigger.rstrip("0123456789")
                    aux_dic[strippedTrigger] = 0  #initialise trigger-dataset overlap counters to zero
                triggerDatasetCorrMatrix[dataset1] = aux_dic.copy()  
            triggerDatasetCorrMatrix[dummy_nonpure] = aux_dic.copy()
            
            if opts.maps == "allmaps":    #Initialize the correlation matrices for the new datasets
                for dataset1 in newDatasetList:
                    aux_dic = {}
                    for dataset2 in newDatasetList:
                        aux_dic[dataset2] = 0
                    newDatasetNewDatasetCorrMatrix[dataset1] = aux_dic.copy()
                    aux_dic={}
                    for trigger in myPaths:
                        strippedTrigger = trigger.rstrip("0123456789")
                        aux_dic[strippedTrigger] = 0
                    triggerNewDatasetCorrMatrix[dataset1] = aux_dic.copy()
                triggerNewDatasetCorrMatrix[dummy_nonpure] = aux_dic.copy()

    #END initializations


    #check if event is in the json range
    runnbr = event.object().id().run()
    runls = event.object().id().luminosityBlock()
    runstr = str((runnbr,runls))
    if not check_json(opts.jsonFile, runnbr, runls, isMC):
        continue
    if not runstr in runAndLsList:
        nLS = nLS +1   #count number of of LS processed
        runAndLsList.append(runstr)   #avoid double counting


    if isL1Accept:
        # Check condition DST_Physics when processing L1Accept PD
        isDSTPhysics=False
        for triggerName in myPaths:
            if(not "DST_Physics_v" in triggerName): continue
            index = names.triggerIndex(triggerName)
            if checkTriggerIndex(triggerName,index,names.triggerNames()):
                if triggerBits.product().accept(index):
                    isDSTPhysics = True   #only keep events where a DSTPhysics trigger fires
        if not isDSTPhysics:
            continue

    iPath = 0       

    #various booleans to avoid double counting the event
    #initialized to False for each iteration of the events loop
    kPassedEventPhysics = False
    kPassedEventScouting = False
    kPassedEventParking = False
    kPassedEventMisc = False
    kPassedEventAnalysis = False
    triggerCountsBool = {}
    triggerCounts = 0  #for the non-pure trigger counts, we need more information than a boolean
    for i in range(0, len(myPaths)):
        triggerCountsBool[myPaths[i]] = False
    if bUseMaps:
        #to avoid double counting the event for the dataset counters, we use an integer rather than a boolean
        #for the non-pure trigger-dataset overlap, a boolean doesn't give enough information
        datasetsLatestCounts = primaryDatasetCounts.fromkeys(primaryDatasetCounts.keys(),0) #latest counts initialized to zero for all datasets
        if opts.maps == "allmaps":
            newDatasetsLatestCounts = newDatasetCounts.fromkeys(newDatasetCounts.keys(),0)
        groupCountsBool = groupCounts.fromkeys(groupCounts.keys(),False)
        streamCountsBool = streamCounts.fromkeys(streamCounts.keys(),False)  #for streams a booleam is enough
        myGroupFired = []  #for groups we need a list of all groups that fired to calculate pure and shared rates
    for triggerName in myPaths:   #loop over all trigger paths existing in the input ROOT file
        index = names.triggerIndex(triggerName)
        if checkTriggerIndex(triggerName,index,names.triggerNames()):
            #checking if the event has been accepted by a given trigger
            if triggerBits.product().accept(index):
                #obsolete
                if "HLT_PFMET" in str(triggerName):
                    metBx.Fill(event.object().bunchCrossing()*1.)

                if "HLT_IsoMu24" in str(triggerName):
                    muonBx.Fill(event.object().bunchCrossing()*1.)
                #END obsolete

                atLeastOneEvent = True
                triggerCountsBool[triggerName] = True   #thanks to this boolean, we'll be able to increment the count for this trigger path later
                if not bUseMaps:
                    #if not using maps, only a few counters need to be tracked
                    if triggerName.startswith("HLT_"): triggerCounts += 1
                    if not kPassedEventMisc:
                        kPassedEventMisc = True
                        nPassed_Misc += 1
                else:
                    #we loop over the dictionary keys to see if the path is in that key, and in case we increase the counter
                    strippedTrigger = triggerName.rstrip("0123456789")  #it's simpler to work only with trigger names stripped of the version numbers at the end
                    if physicsStreamOK(strippedTrigger):    #this function checks if this trigger path belongs to a physics stream
                        triggerCounts += 1  #this integer is used for the pure trigger counts
                    if strippedTrigger in datasets.keys():   #if the trigger path is a key in the trigger->dataset map imported from "Menu_HLT.py"
                        for dataset in datasets[strippedTrigger]:
                            if datasetsLatestCounts[dataset] == 0 :  #avoid double counting the dataset event counts
                                primaryDatasetCounts[dataset] = primaryDatasetCounts[dataset] + 1
                            datasetsLatestCounts[dataset] += 1   #for this specific variable we want to double count
                            if opts.maps == "allmaps":    #calculate counts for the merged datasets
                                newDataset = dataset
                                if dataset in newDatasetMap.keys():
                                    newDataset = newDatasetMap[dataset]
                                if newDatasetsLatestCounts[newDataset] == 0 :
                                    newDatasetCounts[newDataset] += 1
                                newDatasetsLatestCounts[newDataset] += 1
                    if strippedTrigger in groups.keys():   #if the trigger path is a key in the trigger->groups map imported from "Menu_HLT.py"
                        for group in groups[strippedTrigger]:
                            if not physicsStreamOK(strippedTrigger): continue   #we're only interested in "physics" trigger paths for the groups (POGs, PAGs)
                            if group not in myGroupFired:   #avoid double counting
                                myGroupFired.append(group)  #keep track of all groups involved in this event
                                groupCounts[group] = groupCounts[group] + 1  #increase the basic group counter (more complicated counters: pure, shared, increased later)
                    if strippedTrigger in streams.keys():  #if the trigger path is a key in the trigger->stream map imported from "Menu_HLT.py"
                        for stream in streams[strippedTrigger]:
                            if streamCountsBool[stream] == False:   #avoid double counting
                                streamCountsBool[stream] = True
                                streamCounts[stream] += 1
                    
                    #increment counter for paths used in physics analyses
                    #need to keep the "types" map updated
                    if kPassedEventAnalysis == False:
                        if belongsToPAG(strippedTrigger) and physicsStreamOK(strippedTrigger):
                            if ("backup" in types[strippedTrigger]) or ("signal" in types[strippedTrigger]):  #if you don't want to keep the "types" updated, consider removing this condition
                                nPAGAnalysisPath += 1
                                kPassedEventAnalysis = True
                    
                    #counter of all "physics" events
                    if kPassedEventPhysics == False:
                        if physicsStreamOK(strippedTrigger):
                            nPassed_Physics += 1
                            kPassedEventPhysics = True
                    
                    #counter of all scouting events (unreliable when running on HLTPhysics)
                    if kPassedEventScouting == False:
                        if scoutingStreamOK(strippedTrigger):
                            nPassed_Scouting += 1
                            kPassedEventScouting = True

                    #counter of all parking events (unreliable when running on HLTPhysics)
                    if kPassedEventParking == False:
                        if parkingStreamOK(strippedTrigger):
                            nPassed_Parking += 1
                            kPassedEventParking = True

                    #counter of all uncategorized events
                    if kPassedEventMisc == False:
                        if not (parkingStreamOK(strippedTrigger) or scoutingStreamOK(strippedTrigger) or physicsStreamOK(strippedTrigger)):
                            nPassed_Misc += 1
                            kPassedEventMisc = True

        iPath = iPath+1
    for trigger in myPaths:
        if not triggerCountsBool[trigger]: continue   #only keep paths which fired in this event
        myPassedEvents[trigger][0] += 1  #increment the basic trigger counter
        if triggerCounts != 1 or not trigger.startswith("HLT_"): continue   #only proceed if a single "physics" HLT path fired in this event
        myPassedEvents[trigger][1] += 1  #increment the pure trigger counter
        #the pure counter is only incremented when a single "physics" trigger is fired in this event
        #the integer triggerCounts only counts "physics" triggers
        
    if bUseMaps:
        #dataset overlaps
        for dataset1 in primaryDatasetList:
            if datasetsLatestCounts[dataset1] == 0: continue     #only keep datasets where at least one path fired in this event
            for dataset2 in primaryDatasetList:  #2nd loop over datasets
                if datasetsLatestCounts[dataset2] == 0: continue #only keep datasets where at least one path fired in this event
                datasetDatasetCorrMatrix[dataset1][dataset2] += 1  #increment dataset1-dataset2 overlap only once
            for trigger in myPaths:
                if not triggerCountsBool[trigger]: continue   #only keep trigger paths which fired in this event
                strippedTrigger = trigger.rstrip("0123456789")
                triggerDatasetCorrMatrix[dataset1][strippedTrigger] += 1 #increment dataset1-trigger overlap only once
                if strippedTrigger in datasets.keys():
                    if (dataset1 in datasets[strippedTrigger]) and datasetsLatestCounts[dataset1] > 1:
                        #increment non-pure overlap if trigger belongs to dataset1 AND if another trigger belonging to that dataset also fired
                        #Non-pure overlap is a way of checking by how much the rate of the dataset is increased by the addition of this trigger
                        triggerDatasetCorrMatrix[dummy_nonpure][strippedTrigger] += 1
        if opts.maps == "allmaps":
            #overlaps with the merged datasets
            for dataset1 in newDatasetList:
                if newDatasetsLatestCounts[dataset1] == 0: continue
                for dataset2 in newDatasetList:
                    if newDatasetsLatestCounts[dataset2] == 0: continue
                    newDatasetNewDatasetCorrMatrix[dataset1][dataset2] += 1
                for trigger in myPaths:
                    if not triggerCountsBool[trigger]: continue
                    strippedTrigger = trigger.rstrip("0123456789")
                    triggerNewDatasetCorrMatrix[dataset1][strippedTrigger] += 1
                    bUseDummy = False
                    #checking if a trigger belongs to a dataset is more complicated with the mergers
                    #because it could belong to an old unchanged dataset or to a new merged dataset
                    #we need to know this to increment the non-pure overlap
                    if strippedTrigger in datasets.keys():
                        if newDatasetsLatestCounts[dataset1] > 1:  #more than one trigger path belonging to the dataset fired: non-pure
                            if dataset1 in datasets[strippedTrigger]:  #if dataset is an old, unchanged dataset
                                bUseDummy = True
                            elif not (dataset1 in primaryDatasetList):  #if it's not old
                                for old_dataset in newDatasetMap.keys():  #we need to look into the merged datasets
                                    if not (dataset1 in newDatasetMap[old_dataset]): continue
                                    if old_dataset in datasets[strippedTrigger]:
                                        bUseDummy = True
                                        break
                    if bUseDummy: triggerNewDatasetCorrMatrix[dummy_nonpure][strippedTrigger] += 1
        
        if len(myGroupFired) == 1:
            #increment the pure group counter if only one group fired
            groupCountsPure[myGroupFired[0]] = groupCountsPure[myGroupFired[0]] + 1            
        
        for group in myGroupFired:
            #formula for the shared group counter
            groupCountsShared[group] = groupCountsShared[group] + 1./len(myGroupFired)

    nEvents += 1  #number of events passing the JSON selection

nLoop += 1  #total number of events processed, including those failing JSON
outputDir='Jobs'
if isMC: outputDir += "/" + MCdataset

#create csv file with important normalization information
global_info_file =  open('%s/output.global.%s.csv'%(outputDir,final_string), 'w')
global_info_file.write("N_LS, " + str(nLS) + "\n")
global_info_file.write("N_eventsInLoop, " + str(nLoop) + "\n")
global_info_file.write("N_eventsProcessed, " + str(nEvents) + "\n")
global_info_file.close()
    
#We'll only write the results if there's at least one event
if atLeastOneEvent:

    #create csv file for uncategorized HLT paths (this is where all paths go in the "nomaps" option)
    misc_path_file = open('%s/output.path.misc.%s.csv'%(outputDir,final_string), 'w')
    misc_path_file.write("Path, Groups, Type, Total Count, Total Rate (Hz), Pure Count, Pure Rate (Hz)\n")  #1st line with column label
    misc_path_file.write("Total Misc, , , " + str(nPassed_Misc) + ", " + str(nPassed_Misc) +"\n")  #2nd line with total number of uncategorized events


    root_file=ROOT.TFile("%s/histos.%s.root"%(outputDir,final_string),"RECREATE")  #root file used for dataset-dataset and trigger-dataset overlaps
    if bUseMaps:
        #csv file for "physics" paths
        physics_path_file = open('%s/output.path.physics.%s.csv'%(outputDir,final_string), 'w')
        physics_path_file.write("Path, Groups, Type, Total Count, Total Rate (Hz), Pure Count, Pure Rate (Hz)\n")
        physics_path_file.write("Total Physics, , , " + str(nPassed_Physics) + ", " + str(nPassed_Physics) +"\n")
        physics_path_file.write("Total Analysis Physics, , , " + str(nPAGAnalysisPath) + ", " + str(nPAGAnalysisPath) +"\n")
        
        #csv file for scouting paths
        scouting_path_file = open('%s/output.path.scouting.%s.csv'%(outputDir,final_string), 'w')
        scouting_path_file.write("Path, Groups, Type, Total Count, Total Rate (Hz), Pure Count, Pure Rate (Hz)\n")
        scouting_path_file.write("Total Scouting, , , " + str(nPassed_Scouting) + ", " + str(nPassed_Scouting) +"\n")
        
        #csv file for parking paths
        parking_path_file = open('%s/output.path.parking.%s.csv'%(outputDir,final_string), 'w')
        parking_path_file.write("Path, Groups, Type, Total Count, Total Rate (Hz), Pure Count, Pure Rate (Hz)\n")
        parking_path_file.write("Total Parking, , , " + str(nPassed_Parking) + ", " + str(nPassed_Parking) +"\n")
        
        
        
        
        #2d histograms for the correlation plots
        triggerDataset_histo=ROOT.TH2F("trigger_dataset_corr","Trigger-Dataset Correlations",len(primaryDatasetList)+1,0,len(primaryDatasetList)+1,len(myPaths),0,len(myPaths))
        datasetDataset_histo=ROOT.TH2F("dataset_dataset_corr","Dataset-Dataset Correlations",len(primaryDatasetList),0,len(primaryDatasetList),len(primaryDatasetList),0,len(primaryDatasetList))
        
        if opts.maps == "allmaps":
            triggerNewDataset_histo=ROOT.TH2F("trigger_newDataset_corr","New Trigger-Dataset Correlations",len(newDatasetList)+1,0,len(newDatasetList)+1,len(myPaths),0,len(myPaths))
            newDatasetNewDataset_histo=ROOT.TH2F("newDataset_newDataset_corr","New Dataset-Dataset Correlations",len(newDatasetList),0,len(newDatasetList),len(newDatasetList),0,len(newDatasetList))
            triggerNewDataset_file = open('%s/output.trigger_newDataset_corr.%s.csv'%(outputDir,final_string), 'w')
            newDatasetNewDataset_file = open('%s/output.newDataset_newDataset_corr.%s.csv'%(outputDir,final_string), 'w')

        #csv files with trigger-dataset and dataset-dataset overlaps
        #they're not used for plotting, the root file is used instead
        triggerDataset_file = open('%s/output.trigger_dataset_corr.%s.csv'%(outputDir,final_string), 'w')
        datasetDataset_file = open('%s/output.dataset_dataset_corr.%s.csv'%(outputDir,final_string), 'w')
        
        
        #write column and line labels for the overlap csv files
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
        trigger = myPaths[i]
        strippedTrigger = trigger.rstrip("0123456789")
        group_string = ""
        type_string = ""
        if not bUseMaps:
            #note that each counter is written twice, the 1st time in the "counts" column and the 2nd time in the "rates" column
            #the merging and scaling script will turn these 2nd counts into the right normalized rates
            misc_path_file.write('{}, {}, {}, {}, {}, {}, {}'.format(trigger, group_string, type_string, myPassedEvents[trigger][0], myPassedEvents[trigger][0], myPassedEvents[trigger][1], myPassedEvents[trigger][1]))  #fill the lines of the csv file with trigger counts for each path
            misc_path_file.write('\n')
        else:
            #other csv files are only filled if we're using maps
            if strippedTrigger in groups.keys():
                for group in groups[strippedTrigger]:
                    group_string = group_string + group + " "
                for ttype in types[strippedTrigger]:
                    type_string = type_string + ttype + " "
            if physicsStreamOK(strippedTrigger):
                physics_path_file.write('{}, {}, {}, {}, {}, {}, {}'.format(trigger, group_string, type_string, myPassedEvents[trigger][0], myPassedEvents[trigger][0], myPassedEvents[trigger][1], myPassedEvents[trigger][1]))
                physics_path_file.write('\n')
            if scoutingStreamOK(strippedTrigger):
                scouting_path_file.write('{}, {}, {}, {}, {}, {}, {}'.format(trigger, group_string, type_string, myPassedEvents[trigger][0], myPassedEvents[trigger][0], myPassedEvents[trigger][1], myPassedEvents[trigger][1]))
                scouting_path_file.write('\n')
            if parkingStreamOK(strippedTrigger):
                parking_path_file.write('{}, {}, {}, {}, {}, {}, {}'.format(trigger, group_string, type_string, myPassedEvents[trigger][0], myPassedEvents[trigger][0], myPassedEvents[trigger][1], myPassedEvents[trigger][1]))
                parking_path_file.write('\n')
            if not (parkingStreamOK(strippedTrigger) or scoutingStreamOK(strippedTrigger) or physicsStreamOK(strippedTrigger)):
                misc_path_file.write('{}, {}, {}, {}, {}, {}, {}'.format(trigger, group_string, type_string, myPassedEvents[trigger][0], myPassedEvents[trigger][0], myPassedEvents[trigger][1], myPassedEvents[trigger][1]))
                misc_path_file.write('\n')

            #fill trigger-dataset overlap csv file and root histo
            triggerDataset_file.write(strippedTrigger)
            j = 0
            triggerDataset_histo.GetYaxis().SetBinLabel(i+1, strippedTrigger)
            for dataset in primaryDatasetList:
                triggerDataset_file.write(", " + str(triggerDatasetCorrMatrix[dataset][strippedTrigger]))
                triggerDataset_histo.GetXaxis().SetBinLabel(j+1, dataset)
                triggerDataset_histo.SetBinContent(j+1, i+1, triggerDatasetCorrMatrix[dataset][strippedTrigger])
                j += 1
            triggerDataset_histo.GetXaxis().SetBinLabel(j+1, dummy_nonpure)
            triggerDataset_histo.SetBinContent(j+1, i+1, triggerDatasetCorrMatrix[dummy_nonpure][strippedTrigger])
            triggerDataset_file.write(", " + str(triggerDatasetCorrMatrix[dummy_nonpure][strippedTrigger]))
            triggerDataset_file.write("\n")
            
            if opts.maps == "allmaps":
                triggerNewDataset_file.write(strippedTrigger)
                j = 0
                triggerNewDataset_histo.GetYaxis().SetBinLabel(i+1, strippedTrigger)
                for dataset in newDatasetList:
                    triggerNewDataset_file.write(", " + str(triggerNewDatasetCorrMatrix[dataset][strippedTrigger]))
                    triggerNewDataset_histo.GetXaxis().SetBinLabel(j+1, dataset)
                    triggerNewDataset_histo.SetBinContent(j+1, i+1, triggerNewDatasetCorrMatrix[dataset][strippedTrigger])
                    j += 1
                triggerNewDataset_histo.GetXaxis().SetBinLabel(j+1, dummy_nonpure)
                triggerNewDataset_histo.SetBinContent(j+1, i+1, triggerNewDatasetCorrMatrix[dummy_nonpure][strippedTrigger])
                triggerNewDataset_file.write(", " + str(triggerNewDatasetCorrMatrix[dummy_nonpure][strippedTrigger]))
                triggerNewDataset_file.write("\n")
        
    
    misc_path_file.close()
    if bUseMaps:
        physics_path_file.close()
        scouting_path_file.close()
        triggerDataset_file.close()
        if opts.maps == "allmaps":
            triggerNewDataset_file.close()
    
        #files for dataset rates: "physics" and uncategorized
        physics_dataset_file = open('%s/output.dataset.physics.%s.csv'%(outputDir,final_string), 'w')
        misc_dataset_file = open('%s/output.dataset.misc.%s.csv'%(outputDir,final_string), 'w')
        
        physics_dataset_file.write("Dataset, Counts, Rates (Hz)\n")
        misc_dataset_file.write("Dataset, Counts, Rates (Hz)\n")
        i = 0
        for key in primaryDatasetList:
            isPhysicsDataset = False
        
            for trigger in myPaths:
                strippedTrigger = trigger.rstrip("0123456789")
                if not strippedTrigger in datasets.keys(): continue
                if physicsStreamOK(strippedTrigger) and (key in datasets[strippedTrigger]):
                    isPhysicsDataset = True  #a "physics" dataset needs to contain at least one trigger belonging to a "physics" stream
                    break
            if isPhysicsDataset:
                #like for the trigger csv files, the counts are written twice, the extra copy will be normalized later to a proper rate
                physics_dataset_file.write(str(key) + ", " + str(primaryDatasetCounts[key]) +", " + str(primaryDatasetCounts[key]))
                physics_dataset_file.write('\n')
            else:
                misc_dataset_file.write(str(key) + ", " + str(primaryDatasetCounts[key]) +", " + str(primaryDatasetCounts[key]))
                misc_dataset_file.write('\n')
        
            #fill dataset-dataset overlap csv file and root histo
            i += 1
            datasetDataset_file.write(key)
            datasetDataset_histo.GetYaxis().SetBinLabel(i, key)
            j = 0
            for key2 in primaryDatasetList:
                j += 1
                if (primaryDatasetCounts[key] > 0): datasetDatasetCorrMatrix[key2][key] = datasetDatasetCorrMatrix[key2][key] 
                datasetDataset_file.write(", " + str(datasetDatasetCorrMatrix[key2][key]))
                datasetDataset_histo.GetXaxis().SetBinLabel(j, key2)
                datasetDataset_histo.SetBinContent(j, i, datasetDatasetCorrMatrix[key2][key])
            datasetDataset_file.write("\n")
        
        if opts.maps == "allmaps":
            newDataset_file = open('%s/output.newDataset.physics.%s.csv'%(outputDir,final_string), 'w')
            newDataset_file.write("Dataset, Counts, Rates (Hz)\n")
            i = 0
            for key in newDatasetList:
                isPhysicsDataset = False
            
                for trigger in myPaths:
                    strippedTrigger = trigger.rstrip("0123456789")
                    if physicsStreamOK(strippedTrigger):
                        if (key in datasets[strippedTrigger]):
                            isPhysicsDataset = True
                        elif not (key in primaryDatasetList):
                            for old_dataset in newDatasetMap.keys():
                                if not (key in newDatasetMap[old_dataset]): continue
                                if old_dataset in datasets[strippedTrigger]:
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
        
        
        group_file = open('%s/output.group.%s.csv'%(outputDir,final_string),'w')
        group_file.write('Groups, Counts, Rates (Hz), Pure Counts, Pure Rates (Hz), Shared Counts, Shared Rates (Hz)\n')
        for key in groupCounts.keys():
            group_file.write(str(key) + ", " + str(groupCounts[key]) +", " + str(groupCounts[key]) + ", " + str(groupCountsPure[key]) +", " + str(groupCountsPure[key]) + ", " + str(groupCountsShared[key]) +", " + str(groupCountsShared[key]))
            group_file.write('\n')
        
        group_file.close()
        
        stream_file = open('%s/output.stream.%s.csv'%(outputDir,final_string),'w')
        stream_file.write('Streams, Counts, Rates (Hz)\n')
        for stream in streamCounts.keys():
            stream_file.write(str(stream) + ", " + str(streamCounts[stream]) +", " + str(streamCounts[stream]) + "\n")
        
        stream_file.close()
        
    #Save histos into root file
    root_file.cd()
    if bUseMaps:
        triggerDataset_histo.Write()
        datasetDataset_histo.Write()
        if opts.maps == "allmaps":
            triggerNewDataset_histo.Write()
            newDatasetNewDataset_histo.Write()
    metBx.Write()  #obsolete
    muonBx.Write() #obsolete
    root_file.Close()
