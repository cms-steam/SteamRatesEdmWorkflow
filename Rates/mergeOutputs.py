'''
                       Merge outputs from jobs send on the batch queue,
                     scale them to the target luminosity, sort the rates
''' 
import ROOT
import math
import os
from makeListsOfRawOutputs import globalFiles
from makeListsOfRawOutputs import masterDic
from makeListsOfRawOutputs import rootFiles as rootList
from Menu_HLT import datasetStreamMap
from aux import makeIncreasingList
import csv

#Rescaling parameters, set them according to your own needs
lumi_in = 1.06e34 #1.467e34
lumi_target = 1.1e34
HLT_prescale = 10.
LS_length = 23.31 #seconds


#Merge the file with the global rate information (number of events, LS processed)
nLS = 0
n_events = 0
for globalFile in globalFiles:
    with open(globalFile) as ffile:
        reader=csv.reader(ffile, delimiter=',')
        for row in reader:
            if row[0] == "N_LS":
                if not "e" in row[1]:
                    nLS += int(row[1])
            elif row[0] == "N_eventsProcessed":
                n_events += int(row[1])

scaleFactor = lumi_target/lumi_in * HLT_prescale  /  ( nLS * LS_length ) 

mergedGlobal = open ("Results/output.global.csv", "w")
mergedGlobal.write("N_LS, " + str(nLS) + "\n")
mergedGlobal.write("N_eventsProcessed, " + str(n_events) + "\n")
mergedGlobal.write("Scale Factor, "+ str(scaleFactor) + "\n")
mergedGlobal.close()



sorted_stream_list = []

#first we make sure the streams are the first on the keyList
keyList = ["output.stream.csv"]
for key in masterDic:
    if key != "output.stream.csv": keyList.append(key)

for i in range(0, len(keyList)):
    key = keyList[i]
    print key

    mergedFile = open("Results/"+key, "w")
    countsDic = {}
    groupsDic = {}
    firstFile = True
    columnOneIsGroups = False
    lfile = masterDic[key][0]

    for file_in in masterDic[key]:
        ffile = open(file_in)
        reader=csv.reader(ffile, delimiter=',')
        
        if firstFile:
            firstFile = False
            '''
            firstRow = True
            print file_in
        
            for row in reader:
                if firstRow:
                    if len(row) < 2: continue
                    if "Groups" in row[1]: columnOneIsGroups = True
                    firstRow = False
                else:
                    countsDic[row[0]] = []
                    if columnOneIsGroups:
                        groupsDic[row[0]] = row[1]
                        for i in range(2,len(row)):
                            countsDic[row[0]].append(int(row[i]))
                    else:
                        for i in range(1,len(row)):
                            if "." in row[i]:
                                countsDic[row[0]].append(float(row[i]))
                            else:
                                countsDic[row[0]].append(int(row[i]))
            
            '''
            pass    
        else:
            firstRow = True

            for row in reader:
                if firstRow:
                    if len(row) < 2: continue
                    if "Groups" in row[1]: columnOneIsGroups = True
                    firstRow = False
                else:
                    if columnOneIsGroups:
                        if (not row[0] in countsDic.keys()) and len(row[0]) > 1:
                            lfile = file_in
                            countsDic[row[0]] = []
                            groupsDic[row[0]] = row[1]
                            for i in range(2,len(row)):
                                countsDic[row[0]].append(int(row[i]))
                        else:
                            for i in range(2,len(row)):
                                countsDic[row[0]][i-2] += int(row[i])
                    else:
                        if (not row[0] in countsDic.keys()) and len(row[0]) > 1:
                            lfile = file_in
                            countsDic[row[0]] = []
                            for i in range(1,len(row)):
                                if "." in row[i]:
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
    if "_dataset_" in key:
        mmap = countsDic.copy()
        for kkey in mmap:
            sorted_list.append(kkey)
    #sorting the datasets, this should happen AFTER the streams were sorted
    elif "dataset." in key:
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
            
    #sort by decreasing count
    else:
        mmap = countsDic.copy()
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


    if "stream" in key: sorted_stream_list = sorted_list
            

    lastFile = open(lfile)
    print lfile
    reader=csv.reader(lastFile, delimiter=',')               
    firstRow = True
    for row in reader:
        if firstRow:
            firstRow = False
            if "dataset." in key: mergedFile.write("Stream, ")
            mergedFile.write(row[0])
            for i in range(1, len(row)):
                mergedFile.write(", " + row[i])
            mergedFile.write("\n")
        else:
            break
    
    for i in range(0, len(sorted_list)):
        kkey = sorted_list[i]
        if "dataset." in key:
            if kkey in datasetStreamMap.keys():
                mergedFile.write(datasetStreamMap[kkey] + ", ")
            else:
                mergedFile.write("nostream, ")
        mergedFile.write(kkey)
        if columnOneIsGroups: mergedFile.write( " ," + groupsDic[kkey] )
        for i in range(0, len(countsDic[kkey])):
            if "_dataset_" in key:
                mergedFile.write(  ", " + str( round(countsDic[kkey][i]*scaleFactor,2) )  )
            else:
                if i%2 == 0:
                    mergedFile.write(  ", " + str( countsDic[kkey][i]                      )  )
                else:
                    mergedFile.write(  ", " + str( round(countsDic[kkey][i]*scaleFactor,2) )  )
        mergedFile.write("\n")
    mergedFile.close()




#Merge the root files, scale the rates to the target lumi
hadd_text = "hadd -f Results/corr_histos.root"
for rootFile in rootList:
    hadd_text += " " + rootFile
os.system(hadd_text)

root_file=ROOT.TFile("Results/corr_histos.root","UPDATE")
root_file.cd()

tD_histo = root_file.Get("trigger_dataset_corr")
dD_histo = root_file.Get("dataset_dataset_corr")

tD_histo.SetName("trigger_dataset_corr_old")
dD_histo.SetName("dataset_dataset_corr_old")
#sort the triggers by decreasing rates
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


#sorting datasets
sorted_dataset_list = []
dataset_index_map = {}
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
                    continue
            else:
                continue
            if dataset in processed_datasets: continue
            dataset_index_map[dataset] = j
            if dD_histo.GetBinContent(j,j) < min_count :
                min_count = dD_histo.GetBinContent(j,j)
                min_dataset = dD_histo.GetYaxis().GetBinLabel(j)
        if min_dataset != "": sorted_dataset_list.append(min_dataset)
        processed_datasets.append(min_dataset)


tD_histo_sorted = ROOT.TH2F("trigger_dataset_corr", "Trigger-Dataset Correlations", len(sorted_dataset_list), 0, len(sorted_dataset_list), len(sorted_trigger_list), 0, len(sorted_trigger_list))
dD_histo_sorted = ROOT.TH2F("dataset_dataset_corr", "Dataset-Dataset Correlations", len(sorted_dataset_list), 0, len(sorted_dataset_list), len(sorted_dataset_list), 0, len(sorted_dataset_list))


for i in range(0,len(sorted_dataset_list)):
    dataset = sorted_dataset_list[i]
    ii = dataset_index_map[dataset]

    dD_histo_sorted.GetXaxis().SetBinLabel(i+1, dataset)
    tD_histo_sorted.GetXaxis().SetBinLabel(i+1, dataset)
    for j in range(0,len(sorted_dataset_list)):
        dataset2 = sorted_dataset_list[j]
        jj = dataset_index_map[dataset2]
        bin_content = dD_histo.GetBinContent(ii, jj)*scaleFactor
        dD_histo_sorted.SetBinContent(i+1, j+1, bin_content)

        if (i == 0) : dD_histo_sorted.GetYaxis().SetBinLabel(j+1, dataset2)

    for j in range(0,len(sorted_trigger_list)):
        trigger = sorted_trigger_list[j]
        jj = trigger_index_map[trigger]
        bin_content = tD_histo.GetBinContent(ii, jj)*scaleFactor
        tD_histo_sorted.SetBinContent(i+1, j+1, bin_content)

        if (i == 0) : tD_histo_sorted.GetYaxis().SetBinLabel(j+1, trigger)

#Write the sorted histos
root_file.Delete("trigger_dataset_corr;1")
root_file.Delete("dataset_dataset_corr;1")

tD_histo_sorted.Write()
dD_histo_sorted.Write()

root_file.Close()

