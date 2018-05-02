'''
                       Merge outputs from jobs send on the batch queue,
                     scale them to the target luminosity, sort the rates
''' 
import ROOT
import math
import os
import sys
import csv
#from makeListsOfRawOutputs import globalFiles
#from makeListsOfRawOutputs import masterDic
#from makeListsOfRawOutputs import rootFiles as rootList
from Menu_HLT import L1PS as L1PS
from Menu_HLT import HLTPS as HLTPS


# extract L1 PS column labels
def getLabelsL1PS(filename):
  labels = []
  infile = open(filename,'r')
  for line in infile:
    fields = line.split(',')
    labels = fields[2:]
    break
  infile.close()
  return labels

# extract L1 PS table from output file
def getTableL1PS(filename):
  tableL1PS = {}
  infile = open(filename,'r')
  for line in infile:
    if not "L1_" in line:
      continue
    fields = line.split(',')
    tableL1PS[fields[1]] = fields[2:]
  infile.close()
return tableL1PS


file_STEAM = 'Results/output.path.physics.csv'
file_WBM = 'WBM.csv'

mapSTEAM = {}
mapWBM = {}
triggerList = []
with open(file_STEAM) as ffile:
    reader=csv.reader(ffile, delimiter=',')
    for row in reader:
        triggerKey = str(row[0]).rstrip("0123456789")
        mapSTEAM[triggerKey] = [int(row[1]), float(row[2])]
        triggerList.append(str(row[0]))

with open(file_WBM) as ffile:
    reader=csv.reader(ffile, delimiter=',')
    for row in reader:
        triggerKey = str(row[0]).rstrip("0123456789")
        mapWBM[triggerKey] = [int(row[1]), int(row[2]), int(row[3]), float(row[4])]

file_out = open('Results/comparison.csv', 'w')
file_out.write('Paths, STEAM L1 PS, STEAM HLT PS, STEAM Counts, STEAM Rates (Hz), WBM L1 PS, WBM HLT PS, WBM Counts, WBM Rates (Hz), STEAM-WBM (Hz), Error (Hz), Rel. difference, Error, Pull\n')

for i in range(0, len(triggerList)):
    triggerKey = triggerList[i].rstrip("0123456789")
    file_out.write(triggerList[i] + ", " + L1PS[triggerKey] + ", " + HLTPS[triggerKey] + ", " + str(mapSTEAM[triggerKey][0]) + ", " + str(mapSTEAM[triggerKey][1]) + ", ")
    if not (triggerKey in mapWBM.keys()):
        file_out.write("0, 0, 0, 0, 0, 0, 0, 0, 0\n")
    else:
        file_out.write(str(mapWBM[triggerKey][0]) + ", " + str(mapWBM[triggerKey][1]) + ", " + str(mapWBM[triggerKey][2]) + ", " + str(mapWBM[triggerKey][3]) + ", ")
        diff = mapSTEAM[triggerKey][1] - mapWBM[triggerKey][3]
        diff_err = sqrt( mapSTEAM[triggerKey][1]**2/mapSTEAM[triggerKey][0] + mapWBM[triggerKey][3]**2/mapWBM[triggerKey][2] )
        pull = 0
        if diff_err !=0:
            pull = diff/diff_err
        file_out.write(str(diff) + ", " + str(diff_err) + ", ")
        if mapWBM[triggerKey][2] == 0:
            file_out.write("0, 0, " +str(pull)+ "\n")
        else:
            reldiff = diff/mapWBM[triggerKey][3]
            reldiff_err = sqrt( (diff_err/diff)**2 + 1./mapWBM[triggerKey][2] ) * reldiff
            file_out.write(str(reldiff) + ", " + str(reldiff_err) + ", " + str(pull) + "\n")

file_out.close()
