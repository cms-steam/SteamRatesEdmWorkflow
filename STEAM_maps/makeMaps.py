import os, time, string, sys, imp, re

# Indices to read input tsv file
nCol       = 21 # based on 15_0_0 GRun V48 menu
iPath      = 2
iGroup     = 3
iType      = 4
iStatus    = -1
iEnable    = -1
iTarget    = -1
iFlatness  = -1
iStream    = 0
iDataset   = 1

# Get input file
infilename = 'steamdb.tsv'
if len(sys.argv)>1:
    infilename = sys.argv[1]

infile = open(infilename,'r')

prefixes = ['HLT_','AlCa_','DST_','MC_']

# Instantiate maps
streamMap    = {}
datasetMap   = {}
groupMap     = {}
typeMap      = {}
statusMap    = {}
targetMap    = {}
flatnessMap = {}
enableMap    = {}

# Loop over lines in input table
nLines =0
nLines2=0

for line in infile:

    nLines+=1

    # Remove spaces
    line = line.replace(" ","")

    # Split the line in words
    fields = line.split()
    print(fields)
    print(len(fields))
    if len(fields)!=nCol:
        print("WILL NOT PROCESS LINE (len(fields)!=",nCol,"): ", len(fields), "!=", fields)
        continue

    # Get path name
    path = str(fields[iPath])
    path = path.rstrip("0123456789") # fixme custom

    #print "PATH:",path
    
    # Decide to process the line
    process=False
    for pre in prefixes:
        if pre in path:
            process=True
    if not process:
        print("WILL NOT PROCESS LINE (wrong prefix): ", fields)
        continue

    # Fill the maps with 1 entry for this path
    groups2 = []
    types2  = []
    status2 = []
    targets2= []
    flatness2 = []

    theDataset= str(fields[iDataset])
    theStream = str(fields[iStream])
    theEnable = ""
    if (iEnable >= 0): theEnable = str(fields[iEnable])

    # Avoid dataset ParkingScoutingMonitor
    if theDataset=="ParkingScoutingMonitor":
        continue

    # Avoid double entries
    if path in datasetMap:
        nLines2+=1
        print("PATH ALREADY IN MAP: ", path, datasetMap[path], theDataset)
        continue

    streamMap[ path]= [theStream]
    datasetMap[path]= [theDataset]
    if (iEnable >= 0):  enableMap[ path]= [theEnable]

    groups = fields[iGroup].split(',')
    for e in groups:
        if e=='' or e==' ' or e==',':
            continue
        groups2.append(e)
        groupMap[path]  = groups2

    types = fields[iType].split(',')
    for e in types:
        if e=='' or e==' ' or e==',':
            continue
        types2.append(e)
    typeMap[path]   = types2

    if (iStatus >= 0):
        status = fields[iStatus].split(',')
        for e in status:
            if e=='' or e==' ' or e==',':
                continue
            status2.append(e)
        statusMap[path]   = status2

    if (iTarget >= 0):
        targets = fields[iTarget].split(',')
        for e in targets:
            if e=='' or e==' ' or e==',':
                continue
            targets2.append(e)
        targetMap[path]   = targets2
        
    if (iFlatness >= 0):
        flatness = fields[iFlatness].split(',')
        for e in flatness:
            if e=='' or e==' ' or e==',':
                continue
            flatness2.append(e)
        flatnessMap[path]   = flatness2

# Check number of paths
print("Looked at #lines=", nLines, nLines2)
print("Number of paths in maps: ", len(streamMap), len(datasetMap), len(groupMap), len(typeMap), len(statusMap), len(targetMap), len(flatnessMap), len(enableMap))
        
# Write out maps
outputMaps = open("SteamDB.py", "w")

outputMaps.write("streamMap = {\n")
for path in streamMap:
  outputMaps.write("\t'"+path+"': "+str(streamMap[path])+",\n")
outputMaps.write("}\n\n")

outputMaps.write("datasetMap = {\n")
for path in datasetMap:
  outputMaps.write("\t'"+path+"': "+str(datasetMap[path])+",\n")
outputMaps.write("}\n\n")

outputMaps.write("groupMap = {\n")
for path in groupMap:
  outputMaps.write("\t'"+path+"': "+str(groupMap[path])+",\n")
outputMaps.write("}\n\n")

outputMaps.write("typeMap = {\n")
for path in typeMap:
  outputMaps.write("\t'"+path+"': "+str(typeMap[path])+",\n")
outputMaps.write("}\n\n")

outputMaps.write("statusMap = {\n")
for path in statusMap:
  outputMaps.write("\t'"+path+"': "+str(statusMap[path])+",\n")
outputMaps.write("}\n\n")

outputMaps.write("targetMap = {\n")
for path in targetMap:
  outputMaps.write("\t'"+path+"': "+str(targetMap[path])+",\n")
outputMaps.write("}\n\n")

outputMaps.write("flatnessMap = {\n")
for path in flatnessMap:
  outputMaps.write("\t'"+path+"': "+str(flatnessMap[path])+",\n")
outputMaps.write("}\n\n")

outputMaps.write("enableMap = {\n")
for path in enableMap:
  outputMaps.write("\t'"+path+"': "+str(enableMap[path])+",\n")
outputMaps.write("}\n\n")

outputMaps.close()
#
