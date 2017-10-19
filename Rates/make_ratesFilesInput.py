import os
import math
import shlex
import subprocess

files_dir = "/afs/cern.ch/work/d/dbeghin/Work/Rates/SteamRatesEdmWorkflow/Rates/blob"


def runCommand(commandLine):
    #sys.stdout.write("%s\n" % commandLine)                                                                                                                                       
    args = shlex.split(commandLine)
    retVal = subprocess.Popen(args, stdout = subprocess.PIPE)
    return retVal


outfile = open('filesInput.py', 'w')
outfile.write("fileInputNames = [\n")

ls_command = runCommand("ls " + files_dir)
stdout, stderr = ls_command.communicate()

for line in stdout.splitlines():
    if ".root" in line:
        outfile.write('"' + files_dir + '/' + line + '",' + '\n')
outfile.write(']')
