import ROOT
import math
import os
from makeListsOfRawOutputs import globalFiles
from makeListsOfRawOutputs import masterDic
from makeListsOfRawOutputs import rootFiles as rootList
import csv

#Rescaling parameters, set them according to your own needs
lumi_in = 1.216e34
lumi_target = 1.5e34
HLT_prescale = 1160.
LS_length = 23.31 #seconds


nLS = 0
n_events = 0
for globalFile in globalFiles:
    with open(globalFile) as ffile:
        reader=csv.reader(ffile, delimiter=',')
        for row in reader:
            if row[0] == "N_LS":
                nLS += int(row[1])
            elif row[0] == "N_eventsProcessed":
                n_events += int(row[1])

scaleFactor = lumi_target/lumi_in * HLT_prescale  /  ( nLS * LS_length ) 

mergedGlobal = open ("Results/output.global.csv", "w")
mergedGlobal.write("N_LS, " + str(nLS) + "\n")
mergedGlobal.write("N_eventsProcessed, " + str(n_events) + "\n")
mergedGlobal.write("Scale Factor, "+ str(scaleFactor) + "\n")
mergedGlobal.close()


hadd_text = "hadd -f Results/corr_histos.root"
for rootFile in rootList:
    hadd_text += " " + rootFile
os.system(hadd_text)


for key in masterDic:
    mergedFile = open("Results/"+key, "w")
    countsDic = {}
    firstFile = True
    columnOneIsGroups = False

    for file_in in masterDic[key]:
        ffile = open(file_in)
        reader=csv.reader(ffile, delimiter=',')
        
        if firstFile:
            firstRow = True
            print file_in
        
            for row in reader:
                if firstRow:
                    if "Groups" in row[1]: columnOneIsGroups = True
                    firstRow = False
                else:
                    countsDic[row[0]] = []
                    if columnOneIsGroups:
                        for i in range(2,len(row)):
                            countsDic[row[0]].append(int(row[i]))
                    else:
                        for i in range(1,len(row)):
                            if "." in row[i]:
                                countsDic[row[0]].append(float(row[i]))
                            else:
                                countsDic[row[0]].append(int(row[i]))
            
            firstFile = False
        else:
            firstRow = True
        
            for row in reader:
                if firstRow:
                    if "Groups" in row[1]: columnOneIsGroups = True
                    firstRow = False
                else:
                    if columnOneIsGroups:
                        for i in range(2,len(row)):
                            countsDic[row[0]][i-2] += int(row[i])
                    else:
                        for i in range(1,len(row)):
                            if "." in row[i]:
                                countsDic[row[0]][i-1] += float(row[i])
                            else:
                                countsDic[row[0]][i-1] += int(row[i])
    
    lastFile = open(masterDic[key][0])
    reader=csv.reader(lastFile, delimiter=',')               
    firstRow = True
    for row in reader:
        if firstRow:
            firstRow = False
            mergedFile.write(row[0])
            for i in range(1, len(row)):
                mergedFile.write(", " + row[i])
            mergedFile.write("\n")
        else:
            mergedFile.write(row[0])
            if columnOneIsGroups: mergedFile.write( " ," + row[1] )
            for i in range(0, len(countsDic[row[0]])):
                if "_dataset" in key:
                    mergedFile.write(  ", " + str( round(countsDic[row[0]][i]*scaleFactor,2) )  )
                else:
                    if i%2 == 0:
                        mergedFile.write(  ", " + str( countsDic[row[0]][i]                      )  )
                    else:
                        mergedFile.write(  ", " + str( round(countsDic[row[0]][i]*scaleFactor,2) )  )
            mergedFile.write("\n")
    mergedFile.close()


