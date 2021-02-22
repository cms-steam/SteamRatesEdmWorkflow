'''
                       Merge outputs from condor trigger counting jobs
                  Will merge all data outputs, or MC outputs for a single specified dataset
                     Scales outputs to the target luminosity, sorts the rates
''' 
import ROOT
import math
import os
import sys
#from makeListsOfRawOutputs import globalFiles
#from makeListsOfRawOutputs import masterDic
#from makeListsOfRawOutputs import rootFiles as rootList
from aux import makeIncreasingList
from aux import makeListsOfRawOutputs
import csv

from optparse import OptionParser
parser=OptionParser()
parser.set_defaults(figures=False)
parser.add_option("-w","--which",dest="dataMC",type="str",default="data",help="'data' or specify a MC dataset ?")
parser.add_option("-l","--lumiin",dest="lumiIn",type="float",default=-1,help="VALUE corresponding to the average instant lumi in your json",metavar="VALUE")
parser.add_option("-t","--lumitarget",dest="lumiTarget",type="float",default=-1,help="VALUE corresponding to the target instant lumi for which you wish to calculate your rates",metavar="VALUE")
parser.add_option("-p","--hltps",dest="hltPS",type="int",default=-1,help="PRESCALE of the HLT_physics trigger",metavar="PRESCALE")
parser.add_option("-d","--dir",dest="inDir",type="str",default="Results/Data/Raw",help="DIR where the output of the batch jobs are located",metavar="DIR")
parser.add_option("-m","--maps",dest="maps",type="str",default="nomaps",help="ARG='nomaps' (default option, don't use maps to get dataste/groups/etc. rates), 'somemaps' (get dataste/groups/etc. rates but with no study of dataset merging), 'allmaps' (get dataste/groups/etc. rates and also study dataset merging)",metavar="ARG")
parser.add_option("-f", action="store_true", dest="figures")
opts, args = parser.parse_args()

error_text = '\nError: wrong inputs\n'
help_text = '\npython mergeOutputs.py -w <dataMC> -l <lumiin> -t <lumitarget> -p <hltps> -d <dir> -m <maps>\n -f'
help_text += '(optional argument) <dataMC> = write here "data" if running on data or the name of the MC dataset considered, if running on MC\n'
help_text += '(optional) <lumiin> = VALUE corresponding to the average instant lumi in your data json. No need to specify if MC.\n'
help_text += '(mandatory) <lumitarget> = VALUE corresponding to the target instant lumi for which you wish to calculate your rates\n'
help_text += '(optional) <hltps> = PRESCALE of the HLT_physics trigger if running on data. No need to specify for MC.\n'
help_text += '(optional) <dir> = DIR where the output of the trigger counting jobs are located. For MC this needs to include the name of the dataset.'
help_text += '\n(optional) <maps> = "nomaps" (default option, use none of the maps), "somemaps" (use all maps except those related to dataset merging), "allmaps" (use all maps, including dataset merging)\n'
help_text += '\n(optional) -f  : Adding this option merges the root files which are used to produce trigger-dataset and dataset-dataset correlation figures. By default root files are NOT merged\n'

#Check if all necessary inputs are present
if opts.lumiTarget == -1 or (opts.dataMC == "data" and (opts.lumiIn == -1 or opts.hltPS == -1)):
    print error_text
    print help_text
    sys.exit(2)    

#Check which maps need to be used, and import them
bUseMaps = False
if opts.maps == "allmaps":
    bUseMaps = True
    from Menu_HLT import newDatasetMap
    from Menu_HLT import datasetStreamMap
elif opts.maps == "somemaps":
    bUseMaps = True
    from Menu_HLT import datasetStreamMap
elif opts.maps == "nomaps":
    bUseMaps = False
else:
    print "<maps> input '%s' is wrong" %opts.maps
    print error_text
    print help_text
    sys.exit(2)

files_dir = opts.inDir


#Create lists of output files created by the trigger counting jobs
#The masterDic is a dictionary containing all output categories other than "global" or "root" files
masterDic, rootList, globalFiles = makeListsOfRawOutputs(files_dir, opts.figures)
#The 'makeListsOfRawOutputs' function is imported from "aux.py"

#Merge the file with the global rate information (number of events, LS processed)
nLS = 0
n_eventsLoop = 0
n_events = 0
for globalFile in globalFiles:
    with open(globalFile) as ffile:
        reader=csv.reader(ffile, delimiter=',')
        for row in reader:
            if row[0] == "N_LS":
                if not "e" in row[1]:
                    nLS += int(row[1])   #sum number of LS
            elif row[0] == "N_eventsInLoop":
                n_eventsLoop += int(row[1])
            elif row[0] == "N_eventsProcessed":
                n_events += int(row[1])
            

LS_length = 23.31 #seconds
scaleFactor = 0
subDir = "argh"
xs = 0
#Determine the name of the subdirectory where the output of the merge will be stored
#Also determine the scale factor to be applied to the output of the merge
if opts.dataMC == "data":
    if nLS != 0: scaleFactor = opts.lumiTarget/opts.lumiIn * opts.hltPS  /  ( nLS * LS_length ) 
    subDir = "Data"
else:
    subDir = "MC/"
    from map_MCdatasets_xs import datasetCrossSectionMap
    dataset = ""
    for datasetKey in datasetCrossSectionMap.keys():
        dataset = datasetKey
        dataset = dataset.lstrip("/")
        #the dataset provided could contain either "/" or "_" as separators, so we need to consider both cases
        if dataset == opts.dataMC:
            xs = datasetCrossSectionMap[datasetKey]
            subDir += dataset.replace("/","_")
            break
        dataset = dataset.replace("/","_")
        if dataset == opts.dataMC:
            xs = datasetCrossSectionMap[datasetKey]
            subDir += dataset
            break
    #Multiplying by 0.01 = converting from picobarns to 10^-34 cm^2
    if n_events != 0: scaleFactor = opts.lumiTarget*(xs*0.01)/n_events

print '\n\n\n'
print 'this is %s' %opts.dataMC
print 'files_directory = %s' %files_dir
print 'lumi_target = %se34 /cm2/s'%opts.lumiTarget
if opts.dataMC == "data":
    print 'lumi_in = %se34 /cm2/s'%opts.lumiIn
    print 'hlt_PS = %s'%opts.hltPS
else:
    print 'xs = %s pb'%xs
print 'scale_factor = %s\n\n\n'%scaleFactor


if scaleFactor == 0:
    print 'ERROR: ZERO scale_factor: either this dataset is NOT in the file ../MCDatasets/map_MCdatasets_xs.py, or your trigger counting jobs only gave zero counts'
    print 'ZERO scale factor, stopping now...'
else:
    os.system("mkdir Results/%s"%subDir)
    
    #Write the contents of merged 'global' file
    mergedGlobal = open ("Results/%s/output.global.csv"%subDir, "w")
    mergedGlobal.write("N_LS, " + str(nLS) + "\n")
    mergedGlobal.write("N_eventsInLoop, " + str(n_eventsLoop) + "\n")
    mergedGlobal.write("N_eventsProcessed, " + str(n_events) + "\n")
    mergedGlobal.write("Scale Factor, "+ str(scaleFactor) + "\n")
    mergedGlobal.close()
    
    
    
    sorted_stream_list = []
    
    #We create a list of keys to be considered for the 'masterDic'
    #'masterDic' is the dictionary relating the name of the merged output file that will be created to the list of files that needs to be merged
    #It's important that the keys appear in a certain order (mainly: streams before datasets)
    #The one key which we always use whether we use maps or not is 'output.path.misc.csv'
    keyList = ["output.path.misc.csv"]
    if bUseMaps:
        #first we make sure the streams are before the datasets on the keyList
        keyList.append("output.stream.csv")
        for key in masterDic:
            append = True
            if key == "output.stream.csv":
                append = False
            else:
                if opts.maps == "somemaps":
                    if "new" in key:
                        append = False   #do NOT consider the "new" datasets is "somemaps" option (datasets only considered if "allmaps")
            if append: keyList.append(key)
    
    for i in range(0, len(keyList)):
        key = keyList[i]
        #We consider the keys in the order we carefully put them in the key list
    
        mergedFile = open("Results/%s/%s"%(subDir, key), "w")

        #the 'counts' dictionary is used for all keys
        countsDic = {}    

        #the groups and types are only used when the file considered is for individual HLT paths
        #You may consider removing the 'type' information which isn't kept well updated
        groupsDic = {}
        typesDic = {}

        firstFile = True
        columnOneIsGroups = False
        lfile = masterDic[key][0]
    
        for file_in in masterDic[key]:
            ffile = open(file_in)
            reader=csv.reader(ffile, delimiter=',')
            
            firstRow = True
    
            for row in reader:
                if firstRow:
                    if len(row) < 2: continue
                    #Check if the column #1 contains group information
                    #in this case, we are considering files for individual HLT paths
                    if "Groups" in row[1]: columnOneIsGroups = True
                    firstRow = False
                else:
                    if columnOneIsGroups:
                        if (not row[0] in countsDic.keys()) and len(row[0]) > 1:
                            lfile = file_in
                            countsDic[row[0]] = []
                            groupsDic[row[0]] = row[1]
                            typesDic[row[0]] = row[2]
                            for i in range(3,len(row)):
                                #Two different counts are present for each individual HLT path: total and pure
                                #There are two copies for each of the two counts
                                #One of the copies will be scaled by the scae factor, the other will be left as is, in order to show the user the pure counts
                                #(allows for an estimation of the statistical uncertainty)
                                countsDic[row[0]].append(int(row[i]))
                        else:
                            for i in range(3,len(row)):
                                #Two different counts are present for each individual HLT path: total and pure
                                countsDic[row[0]][i-3] += int(row[i])
                    else:
                        if (not row[0] in countsDic.keys()) and len(row[0]) > 1:
                            lfile = file_in
                            countsDic[row[0]] = []
                            for i in range(1,len(row)):
                                #Several counts may be present in the file, e.g. groups pure, groups total, group shared
                                #Again there are are two copies for each of the counts
                                if "." in row[i]:  #check if the number is a float
                                    countsDic[row[0]].append(float(row[i]))
                                else:
                                    countsDic[row[0]].append(int(row[i]))
                        else:
                            for i in range(1,len(row)):
                                if "." in row[i]:
                                    countsDic[row[0]][i-1] += float(row[i])
                                else:
                                    countsDic[row[0]][i-1] += int(row[i])
    
        
    
        sorted_list = []
    
        #we don't bother sorting the large correlation matrices
        if ("_dataset_" in key) or ("_newDataset_" in key):
            mmap = countsDic.copy()
            for kkey in mmap:
                sorted_list.append(kkey)
        #sorting the datasets, this should happen AFTER the streams were sorted
        #this is why the order of the keys is so important
        elif "dataset." in key:
            #Datasets are grouped together by stream, and within each stream ordered by decreasing count
            for j in range(0,len(sorted_stream_list)):
                stream = sorted_stream_list[j]
                mmap = countsDic.copy()
    
                for dataset in countsDic:
                    if dataset in datasetStreamMap.keys():
                        if not stream == datasetStreamMap[dataset]:
                            del mmap[dataset]
                    else:
                        del mmap[dataset]
    
                while len(mmap) > 0:
                    mmax = -1
                    max_key = ""
                    for dataset in mmap:
                        index = len(mmap[dataset]) - 1
                        if mmap[dataset][index] > mmax: 
                            mmax = mmap[dataset][index]
                            max_key = dataset
                    sorted_list.append(max_key)
                    del mmap[max_key]            
                
        #sort keys other than datasets by decreasing count
        else:
            mmap = countsDic.copy()
            while len(mmap) > 0:
                mmax = -1
                max_key = ""
                for item in mmap:
                    index = len(mmap[item]) - 1
                    if ".path." in key: index = len(mmap[item]) - 4
                    if mmap[item][index] > mmax: 
                        mmax = mmap[item][index]
                        max_key = item
                sorted_list.append(max_key)
                del mmap[max_key]            
    
    
        #intialise stream list for the datasets
        if "stream" in key: sorted_stream_list = sorted_list
                
    
        lastFile = open(lfile)
        reader=csv.reader(lastFile, delimiter=',')               
        firstRow = True
        #Write the first row of the merged files
        for row in reader:
            if firstRow:
                firstRow = False
                if "dataset." in key:
                    mergedFile.write("Stream, ")
                elif ".path." in key:
                    mergedFile.write("Path (w/ version number), ")
                mergedFile.write(row[0])
                for i in range(1, len(row)):
                    mergedFile.write(", " + row[i])
                mergedFile.write("\n")
            else:
                break
        
        total_count = 0
        total_rate = 0
        for i in range(0, len(sorted_list)):
            kkey = sorted_list[i]
            wkey = kkey
            if "dataset." in key:
                if kkey in datasetStreamMap.keys():
                    mergedFile.write(datasetStreamMap[kkey] + ", ")
                else:
                    mergedFile.write("nostream, ")
            elif ".path." in key:
                mergedFile.write(wkey + ", ")
                #we want a copy of the path name to be written without the version number at the end
                #this allows easy comparisons between different versions of the same path
                wkey = wkey.rstrip("0123456789") 
            mergedFile.write(wkey)
            #group names and types are written if the info is present in the file (i.e. it's a file for individual HLT paths)
            if columnOneIsGroups: mergedFile.write( ", " + groupsDic[kkey] + ", " +typesDic[kkey] )
            for i in range(0, len(countsDic[kkey])):
                if ("_dataset_" in key) or ("_newDataset_" in key):
                    #these are the overlap files
                    #here we don't keep the raw counts because it would be awkward to show them together with the scaled numbers on a 2D file
                    mergedFile.write(  ", " + str( round(countsDic[kkey][i]*scaleFactor,2) )  )
                else:
                    #for the other files, we show both the raw counts and the scaled counts
                    if i%2 == 0:
                        #in these columns we keep the raw counts
                        mergedFile.write(  ", " + str( countsDic[kkey][i]                      )  )
                        total_count +=                 countsDic[kkey][i]
                    else:
                        #in the other columns we scale the raw counts by the scale factor we calculated
                        mergedFile.write(  ", " + str( round(countsDic[kkey][i]*scaleFactor,2) )  )
                        total_rate +=                        countsDic[kkey][i]*scaleFactor
            mergedFile.write("\n")
        if "dataset." in key:
            #Write the total dataset count and rate (scaled count)
            mergedFile.write(", Sum, %s, %s\n" %(total_count, round(total_rate,2)) )
        mergedFile.close()
    
    
    
    
    #Merge the root files, scale the rates to the target lumi
    #(If you've chosen to make the figures)
    if opts.figures:
        hadd_text = "hadd -f Results/%s/histos.root"%subDir
        for rootFile in rootList:
            hadd_text += " " + rootFile
        os.system(hadd_text)
    
        if bUseMaps:
            root_file=ROOT.TFile("Results/%s/histos.root"%subDir,"UPDATE")
            root_file.cd()
            
            tD_histo = root_file.Get("trigger_dataset_corr")
            dD_histo = root_file.Get("dataset_dataset_corr")
            if opts.maps == "allmaps":
                newtD_histo = root_file.Get("trigger_newDataset_corr")
                newdD_histo = root_file.Get("newDataset_newDataset_corr")
            
            
            
            #sort the triggers by decreasing rates and finding in which index (bin number) each of them can be found
            trigger_map = {}
            trigger_index_map = {}
            
            for j in range(1,tD_histo.GetNbinsY()+1):
                trigger = tD_histo.GetYaxis().GetBinLabel(j)
                trigger_index_map[trigger] = j
                max_count = -1
                for i in range(1,tD_histo.GetNbinsX()+1):
                    if (tD_histo.GetBinContent(i,j) > max_count): max_count = tD_histo.GetBinContent(i,j)
                trigger_map[trigger] = max_count
            
            sorted_trigger_list = makeIncreasingList(trigger_map)
            
            #sorting datasets and finding in which index (bin number) each of them can be found
            sorted_dataset_list = []
            dataset_index_map = {}
            if opts.maps == "allmaps":
                sorted_newDataset_list = []
                newDataset_index_map = {}
            for i in range(len(sorted_stream_list)-1,-1,-1):
                stream = sorted_stream_list[i]
                processed_datasets = []
                while len(processed_datasets) < dD_histo.GetNbinsY():
                    min_count = 99999000000000
                    min_dataset = ""
                    for j in range(1,dD_histo.GetNbinsY()+1):
                        dataset = dD_histo.GetYaxis().GetBinLabel(j)
                        if dataset in datasetStreamMap.keys():
                            if stream != datasetStreamMap[dataset]:
                                #we want only datasets from the same stream to be together
                                continue
                        else:
                            continue
                        if dataset in processed_datasets: continue
                        dataset_index_map[dataset] = j  #say at which index (bin number) the dataset can be found
                        if dD_histo.GetBinContent(j,j) < min_count :
                            min_count = dD_histo.GetBinContent(j,j)
                            min_dataset = dD_histo.GetYaxis().GetBinLabel(j)
                    if min_dataset != "": sorted_dataset_list.append(min_dataset)  #datasets ordered by stream, then count within each stream
                    processed_datasets.append(min_dataset)
                    if opts.maps == "allmaps":
                        newDataset = min_dataset
                        if min_dataset in newDatasetMap.keys():
                            newDataset = newDatasetMap[min_dataset]
                        if newDataset != "" and not (newDataset in sorted_newDataset_list):
                            sorted_newDataset_list.append(newDataset)
            
            sorted_dataset_list.append("NonPure")
            if opts.maps == "allmaps":
                sorted_newDataset_list.append("NonPure")
            dataset_index_map["NonPure"] = tD_histo.GetNbinsX()  #the non-pure rate is the last one
            
            if opts.maps == "allmaps":
                for jj in range(1,newtD_histo.GetNbinsX()+1):
                    dataset = newtD_histo.GetXaxis().GetBinLabel(jj)
                    newDataset_index_map[dataset] = jj                
            
            
            
            tD_histo_sorted = ROOT.TH2F("trigger_dataset_corr_2", "Trigger-Dataset Correlations", len(sorted_dataset_list), 0, len(sorted_dataset_list), len(sorted_trigger_list), 0, len(sorted_trigger_list))
            dD_histo_sorted = ROOT.TH2F("dataset_dataset_corr_2", "Dataset-Dataset Correlations", len(sorted_dataset_list)-1, 0, len(sorted_dataset_list)-1, len(sorted_dataset_list)-1, 0, len(sorted_dataset_list)-1)
            
            
            #Write the scaled counts (=rates) in each of the bins of the overlap histograms
            #Set the bin labels correctly
            for i in range(0,len(sorted_dataset_list)):
                dataset = sorted_dataset_list[i]
                ii = dataset_index_map[dataset]
            
                if dataset != "NonPure": dD_histo_sorted.GetXaxis().SetBinLabel(i+1, dataset)
                tD_histo_sorted.GetXaxis().SetBinLabel(i+1, dataset)  #set the bin label to the correct dataset
                for j in range(0,len(sorted_dataset_list)-1):
                    if dataset == "NonPure": break
                    dataset2 = sorted_dataset_list[j]
                    jj = dataset_index_map[dataset2]
                    bin_content = dD_histo.GetBinContent(ii, jj)*scaleFactor
                    dD_histo_sorted.SetBinContent(i+1, j+1, bin_content)
            
                    #set bin label to the correct dataset (we only need to do it once for each of them)
                    if (i == 0) : dD_histo_sorted.GetYaxis().SetBinLabel(j+1, dataset2)
            
                for j in range(0,len(sorted_trigger_list)):
                    trigger = sorted_trigger_list[j]
                    jj = trigger_index_map[trigger]
                    bin_content = tD_histo.GetBinContent(ii, jj)*scaleFactor
                    tD_histo_sorted.SetBinContent(i+1, j+1, bin_content)
            
                    #set bin label to the correct trigger (we only need to do it once for each of them)
                    if (i == 0) : tD_histo_sorted.GetYaxis().SetBinLabel(j+1, trigger)
            
            
            #Now do the same for the new datasets if needed
            if opts.maps == "allmaps":
                newtD_histo_sorted = ROOT.TH2F("trigger_newDataset_corr_2", "New Trigger-Dataset Correlations", len(sorted_newDataset_list), 0, len(sorted_newDataset_list), len(sorted_trigger_list), 0, len(sorted_trigger_list))
                newdD_histo_sorted = ROOT.TH2F("newDataset_newDataset_corr_2", "New Dataset-Dataset Correlations", len(sorted_newDataset_list)-1, 0, len(sorted_newDataset_list)-1, len(sorted_newDataset_list)-1, 0, len(sorted_newDataset_list)-1)
                
                
                for i in range(0,len(sorted_newDataset_list)):
                    dataset = sorted_newDataset_list[i]
                    ii = newDataset_index_map[dataset]
                
                    if dataset != "NonPure": newdD_histo_sorted.GetXaxis().SetBinLabel(i+1, dataset)
                    newtD_histo_sorted.GetXaxis().SetBinLabel(i+1, dataset)
                    for j in range(0,len(sorted_newDataset_list)-1):
                        if dataset == "NonPure": break
                        dataset2 = sorted_newDataset_list[j]
                        jj = newDataset_index_map[dataset2]
                        bin_content = newdD_histo.GetBinContent(ii, jj)*scaleFactor
                        newdD_histo_sorted.SetBinContent(i+1, j+1, bin_content)
                
                        if (i == 0) : newdD_histo_sorted.GetYaxis().SetBinLabel(j+1, dataset2)
                
                    for j in range(0,len(sorted_trigger_list)):
                        trigger = sorted_trigger_list[j]
                        jj = trigger_index_map[trigger]
                        bin_content = newtD_histo.GetBinContent(ii, jj)*scaleFactor
                        newtD_histo_sorted.SetBinContent(i+1, j+1, bin_content)
                
                        if (i == 0) : newtD_histo_sorted.GetYaxis().SetBinLabel(j+1, trigger)
            
            
            #Delete the old histos and write the sorted histos
            root_file.Delete("trigger_dataset_corr;1")
            root_file.Delete("dataset_dataset_corr;1")
            
            if opts.maps == "allmaps":
                root_file.Delete("trigger_newDataset_corr;1")
                root_file.Delete("newDataset_newDataset_corr;1")
            
            tD_histo_sorted.SetName("trigger_dataset_corr")
            dD_histo_sorted.SetName("dataset_dataset_corr")
            tD_histo_sorted.Write()
            dD_histo_sorted.Write()
            
            if opts.maps == "allmaps":
                newtD_histo_sorted.SetName("trigger_newDataset_corr")
                newdD_histo_sorted.SetName("newDataset_newDataset_corr")
                newtD_histo_sorted.Write()
                newdD_histo_sorted.Write()
            
            root_file.Close()

