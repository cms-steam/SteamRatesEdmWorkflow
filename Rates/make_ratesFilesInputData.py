import os
import math
import shlex
import subprocess
from aux import runCommand

#Script to automatically generate a list of data input files with the proper format
#To run this script: python make_ratesFilesInputData.py -i /path/to/input/dir


from optparse import OptionParser
parser=OptionParser()
parser.add_option("-i","--indir",dest="inDir",type="str",default="/eos/cms/store/group/dpg_trigger/comm_trigger/TriggerStudiesGroup/STEAM/xulyu/menu_v4.2/HLTPhysics_PS1p5e34_new",help="DIR where the input files are located",metavar="DIR")



opts, args = parser.parse_args()
print 'input directory = %s'%opts.inDir

outfile = open('filesInputData.py', 'w')
outfile.write("fileInputNames = [\n")  #write the first line of 'filesInputData.py'

keepGoing = True
nLoop=0
max_layers=10   #maximum number of layers of subdirectories the code will consider. 10 should be more than enough
lookhere = []   #list of directories under consideration at any time
lookhere.append(opts.inDir)
while(keepGoing):
    lh_buffer = []
    for i in range(0, len(lookhere)):
        dirr = lookhere[i]
        ls_command = runCommand("ls " + dirr)
        stdout, stderr = ls_command.communicate()
        #consider all lines showing up after you do "ls" in the directory
        for line in stdout.splitlines():
            #the code assumes that the only things in these directories are ROOT files, log files, and other directories
            #the code may crash if given an inappropriate directory :(
            if ".root" in line:
                #if it's a ROOT file, update 'filesInputData.py'
                outfile.write('"' + dirr + '/' + line + '",' + '\n')
            elif "log" in line:
                #do nothing here
                continue
            else:
                #otherwise we assume it's a directory and add it to the list of directories considered
                lh_buffer.append(dirr + '/' + line)
    lookhere = list(lh_buffer)
    nLoop += 1  #increment so that we can't stay in this while loop forever
    if len(lookhere)==0 or nLoop>=max_layers: keepGoing = False  #stop when no more directories need to be considered
    if nLoop >= max_layers: print "Too many directory layers (>=%s) before finding any root files, stopping now..." %max_layers

outfile.write(']')
