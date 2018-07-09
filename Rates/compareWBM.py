'''
                       Merge outputs from jobs send on the batch queue,
                     scale them to the target luminosity, sort the rates
''' 
import math
import os
import sys
import csv
#from makeListsOfRawOutputs import globalFiles
#from makeListsOfRawOutputs import masterDic
#from makeListsOfRawOutputs import rootFiles as rootList
from Menu_HLT import psL1TMap as L1PS
from Menu_HLT import psHLTMap as HLTPS


file_STEAM = 'Results/output.path.physics.csv'
file_WBM = 'WBM.csv'
file_pscolumn = 'pscolumn.txt'

mapSTEAM = {}
mapWBM = {}
triggerList = []
with open(file_STEAM) as ffile:
    reader=csv.reader(ffile, delimiter=',')
    firstRow = True
    for row in reader:
        if firstRow:
            firstRow = False
            continue
        triggerKey = str(row[0]).rstrip("0123456789 ")
        if "Total " in triggerKey: continue
        counts_column = 4
        rates_column = 5
        mapSTEAM[triggerKey] = [int(row[counts_column]), float(row[rates_column])]
        triggerList.append(str(row[0]))

with open(file_WBM) as ffile:
    reader=csv.reader(ffile, delimiter=',')
    firstRow = True
    for row in reader:
        if firstRow:
            firstRow = False
            continue
        triggerKey = str(row[0]).rstrip("0123456789 ")
        L1PS_column = 1
        HLTPS_column = 2
        counts_column = 3
        rates_column = 4
        mapWBM[triggerKey] = [int(row[L1PS_column]), int(row[HLTPS_column]), int(row[counts_column]), float(row[rates_column])]

ps_column = -1
with open(file_pscolumn) as ffile:
    ps_column = int(ffile.read())

file_out = open('Results/comparison.csv', 'w')
file_out.write('Paths, STEAM L1 PS, STEAM HLT PS, STEAM Counts, STEAM Rates (Hz), WBM L1 PS, WBM HLT PS, WBM Counts, WBM Rates (Hz), STEAM-WBM (Hz), Error (Hz), Rel. difference, Error, Pull\n')

for i in range(0, len(triggerList)):
    triggerKey = triggerList[i].rstrip("0123456789 ")
    triggerKey2 = triggerKey
    for key in HLTPS.keys():
      if triggerKey in key:
        triggerKey2 = key
        break
    file_out.write(triggerList[i] + ", " + str(L1PS[triggerKey2][ps_column]) + ", " + str(HLTPS[triggerKey2][ps_column]) + ", " + str(mapSTEAM[triggerKey][0]) + ", " + str(mapSTEAM[triggerKey][1]) + ", ")
    if not (triggerKey in mapWBM.keys()):
        file_out.write("0, 0, 0, 0, 0, 0, 0, 0, 0\n")
    else:
        file_out.write(str(mapWBM[triggerKey][0]) + ", " + str(mapWBM[triggerKey][1]) + ", " + str(int(mapWBM[triggerKey][2])) + ", " + str(round(mapWBM[triggerKey][3], 2)) + ", ")
        diff = mapSTEAM[triggerKey][1] - mapWBM[triggerKey][3]
        diff_err = -1
        if mapSTEAM[triggerKey][0] == 0 or mapWBM[triggerKey][2] == 0:
            diff_err = 0
        else:
            diff_err = math.sqrt( mapSTEAM[triggerKey][1]**2/mapSTEAM[triggerKey][0] + mapWBM[triggerKey][3]**2/mapWBM[triggerKey][2] )
        pull = 0
        if diff_err !=0:
            pull = diff/diff_err
        file_out.write(str(round(diff, 2)) + ", " + str(round(diff_err, 2)) + ", ")
        if mapWBM[triggerKey][2] == 0 or diff == 0 or mapWBM[triggerKey][3] == 0:
            file_out.write("0, 0, " +str(pull)+ "\n")
        else:
            reldiff = diff/mapWBM[triggerKey][3]
            reldiff_err = math.sqrt( (diff_err/diff)**2 + 1./mapWBM[triggerKey][2] ) * abs(reldiff)
            file_out.write(str(round(reldiff, 2)) + ", " + str(round(reldiff_err, 2)) + ", " + str(round(pull, 2)) + "\n")

file_out.close()
