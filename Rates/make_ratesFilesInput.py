import os
import math
import shlex
import subprocess
from aux import runCommand


from optparse import OptionParser
parser=OptionParser()
parser.add_option("-i","--indir",dest="inDir",type="str",default="/eos/cms/store/group/dpg_trigger/comm_trigger/TriggerStudiesGroup/STEAM/xulyu/menu_v4.2/HLTPhysics_PS1p5e34_new",help="DIR where the input files are located",metavar="DIR")

opts, args = parser.parse_args()
print 'input directory = %s'%opts.inDir

outfile = open('filesInput.py', 'w')
outfile.write("fileInputNames = [\n")

ls_command = runCommand("ls " + opts.inDir)
stdout, stderr = ls_command.communicate()

for line in stdout.splitlines():
    if ".root" in line:
        outfile.write('"' + opts.inDir + '/' + line + '",' + '\n')
outfile.write(']')
