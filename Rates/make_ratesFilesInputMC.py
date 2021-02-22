import os
import math
import shlex
import subprocess
from aux import runCommand

#Script to automatically generate the list of input files for MC.
#python make_ratesFilesInputMC.py -i /full/path/to/input/dir
#Provide as input directory the directory where all subdirectories are the different MC datasets considered


from optparse import OptionParser
parser=OptionParser()
parser.add_option("-i","--indir",dest="inDir",type="str",default="/eos/cms/store/group/dpg_trigger/comm_trigger/TriggerStudiesGroup/STEAM/xulyu/menu_v4.2/HLTPhysics_PS1p5e34_new",help="DIR where the input files are located",metavar="DIR")

opts, args = parser.parse_args()
print 'input directory = %s'%opts.inDir

outfile = open('filesInputMC.py', 'w')
outfile.write("datasetFilesMap={\n")

#copy MC datasets file here so it can be used
os.system("cp ../MCDatasets/map_MCdatasets_xs.py .")

from map_MCdatasets_xs import datasetCrossSectionMap

keepGoing = True
nLoop=0
max_layers=10
lookhere = []
lookhere.append(opts.inDir)
while(keepGoing):
    lh_buffer = []
    for i in range(0, len(lookhere)):
        dirr = lookhere[i]
        ls_command = runCommand("ls " + dirr)
        stdout, stderr = ls_command.communicate()
        dataset=""
        fileNames=""
        for line in stdout.splitlines():
            if ".root" in line:
                #if we find a ROOT file, we look for the dataset name in the file path
                #the dataset name should be the last subdirectory before the file
                #we find the last subdirectory by looking for the last slash in the full path to the ROOT 
                fileNames += '        "' + dirr + '/' + line + '",' + '\n'
                if dataset == "":
                    lastSlash = dirr.rfind('/')
                    for key in datasetCrossSectionMap.keys():
                        kkey = key.lstrip('/')
                        #the dataset name is given with "_" rather than slashes when it's a directory, because slashes have a special meaning!
                        #so we need to replace "/" with "_" to go from the official dataset name to its modified version
                        #then a proper comparison can be made
                        if kkey.replace('/', '_') == dirr[lastSlash+1:]:  
                            dataset = '    "'+dirr[lastSlash+1:]+'"'
                            break                    
            elif "log" in line:
                continue
            else:
                lh_buffer.append(dirr + '/' + line)
        if dataset != "": outfile.write(dataset+': [\n'+fileNames+'    ],\n')  #we write the name of the dataset together with the file names we just found
    lookhere = list(lh_buffer)
    nLoop += 1
    if len(lookhere)==0 or nLoop>=max_layers: keepGoing = False
    if nLoop >= max_layers: print "Too many directory layers (>=%s) before finding any root files, stopping now..." %max_layers

outfile.write('}\n')
