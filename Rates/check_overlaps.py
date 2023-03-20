# python3 check_overlaps.py

import ROOT
from DataFormats.FWLite import Handle, Events
import math
import os
import csv
import json
import sys, getopt


#Input the necessary Information

pathName = 'HLT_QuadPFJet70_50_40_35_PFBTagParticleNet_2BTagSum0p65_v3'

lumiTarget = 2.0  # Target Lumi

lumiIn = 2.057    # Avg Input Lumi

jsonFile = '/afs/cern.ch/work/s/savarghe/public/Run3Rates/json_362616.txt' 

hltPS = 464

maxEvents = -1 #maxEvents to process

folder = "/eos/cms/store/group/dpg_trigger/comm_trigger/TriggerStudiesGroup/STEAM/savarghe/PU60Fill" #directory with hlt.root files

files = []
for f in os.listdir(folder):
    if f.endswith(".root"):
        files.append("file:"+folder+'/'+f)
#print(files)
events = Events(files)
print("number of events",events.size())

triggerBits, triggerBitLabel = Handle("edm::TriggerResults"), ("TriggerResults::MYHLT")

nLoop = -1

sf = 0



# Helper function to strip number from trigger name
def strip_filename(filename):
    root, ext = os.path.splitext(filename)
    root = root.rstrip('0123456789')
    return root + ext

# Helper function to check if trigger index is valid
def checkTriggerIndex(name,index, names):
    if not 'firstTriggerError' in globals():
        global firstTriggerError
        firstTriggerError = True
    if index>=names.size():
        if firstTriggerError:
            for tr in names: print(tr)
            print()
            print(name," not found!")
            print()
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

Paths_to_check = [pathName,pathName]
myPaths = []
triggerEvents = {}
runAndLsList = []

nLS = 0
# Loop over events and store event numbers that pass each trigger
for event in events:
    nLoop += 1
#for nLoop, event in enumerate(chain):
    if nLoop%100000==0:
        print("Processing entry ",nLoop)

    # Stop looping if maxEvents is reached
    if maxEvents>0 and nLoop >= maxEvents:
        break

    # Get trigger information
    event.getByLabel(triggerBitLabel, triggerBits)
    names = event.object().triggerNames(triggerBits.product())
     #check if event is in the json range
    runnbr = event.object().id().run()
    runls = event.object().id().luminosityBlock()
    runstr = str((runnbr,runls))
    if not check_json(jsonFile, runnbr, runls):
        continue
    if not runstr in runAndLsList:
        nLS = nLS +1
        runAndLsList.append(runstr)

    if nLoop<1:
        for name in names.triggerNames():
            name = str(name)
            strippedTrigger = strip_filename(name)
#            name= strip_filename(name)
            if ("HLTriggerFirstPath" in name) or ("HLTriggerFinalPath" in name): continue
            myPaths.append(name)
    # Check if any of the paths in Paths_to_check fired
    fired = False
    for path in Paths_to_check:
        index = names.triggerIndex(path)
        if checkTriggerIndex(path,index,names.triggerNames()):
            if triggerBits.product().accept(index):
                fired = True
                break

    if not fired:
        continue
    for i, path1 in enumerate(Paths_to_check):
        if path1 not in triggerEvents:
            triggerEvents[path1] = []
        for j, path2 in enumerate(myPaths):
            if i == j:
                continue
            if path2 not in triggerEvents:
                triggerEvents[path2] = []
            index = names.triggerIndex(path2)
            if checkTriggerIndex(path2,index,names.triggerNames()):
                if triggerBits.product().accept(index):
                    triggerEvents[path2].append(nLoop)
                    if path1 == path2:
                        triggerEvents[path1].append(nLoop)

print(len(triggerEvents[path1])/4)

counts = len(triggerEvents[path1])/4

print("Number of Lumissections =  " ,nLS)

LS_length = 23.31 #seconds

#hltPS = 500

sf = lumiTarget/lumiIn * hltPS  /( nLS * LS_length ) 




#Calculate and print the overlap fractions for each pair of paths
overlap_fractions = {}
#overlap_counts = {}
for path1 in Paths_to_check:
    for path2 in myPaths:
        overlap = set(triggerEvents[path1]).intersection(set(triggerEvents[path2]))
        overlap_fraction = 400*len(overlap)/len(triggerEvents[path1])
        overlap_fractions[(path1, path2)] = overlap_fraction
#        overlap_counts[(path1, path2)] = len(overlap)





#overlap_counts = len(overlap)
top_overlaps = sorted(overlap_fractions.items(), key=lambda x: x[1], reverse=True)[:10]
print("Top ten overlap fractions:")
for (path1, path2), overlap_fraction in top_overlaps:
    print("Overlap fraction between {} and {}: {} ".format(path1, path2, overlap_fraction))
# Set up output file name and open file for writing
output_file = "overlap_fractions.txt"
with open(output_file, "w") as f:

    # Loop over each pair of paths and write overlap fraction to file
    for path_pair, overlap_fraction in overlap_fractions.items():
        f.write(f"{path_pair[0]} vs {path_pair[1]}: {overlap_fraction:.2f}\n")

sorted_fractions = sorted(overlap_fractions.items(), key=lambda x: x[1], reverse=True)

# Write the results to a CSV file
with open('overlap_{}.csv'.format(pathName), mode='w', newline='') as file:
    writer = csv.writer(file)
    writer.writerow(['Target path', 'Path2','Overlap(Hz)','Overlap Fraction (%)'])
    for paths, overlap in sorted_fractions:
        path1, path2 = paths
        writer.writerow([path1, path2, round(sf*counts*overlap*0.01,2), round(overlap, 1)])
