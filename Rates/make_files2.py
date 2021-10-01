import os
import math
import shlex
import subprocess

Run = "Run2017C"
run_numbers = [
"301567",
]
datasets = [
"HLTPhysics1",
"HLTPhysics2",
"HLTPhysics3",
"HLTPhysics4",
"HLTPhysics5",
"HLTPhysics6",
"HLTPhysics7",
"HLTPhysics8",
]
tier0_path = "/eos/cms/tier0/store/data"

#Auxiliary function to run on the command line
def runCommand(commandLine):
    #sys.stdout.write("%s\n" % commandLine)
    args = shlex.split(commandLine)
    retVal = subprocess.Popen(args, stdout = subprocess.PIPE)
    return retVal


outfile = open('files2.py', 'w')
outfile.write("inputFileNames = [\n")
for run_nbr in run_numbers:
    for dataset in datasets:
        paths = []
        paths.append( tier0_path + "/" + Run + "/" + dataset + "/RAW/" )
        #now we look for the run number
        keep_looking = True
        time_out = 0
        max_loop = 5 #this is the maximum number of times we'll enter the following loop
        full_path = ""
        while keep_looking:
            for path in paths:
                print(paths)
                ls_command = runCommand("ls " + path)
                stdout, stderr = ls_command.communicate()
                status = ls_command.returncode
                if status != 0:
                    raise IOError("File/path = %s does not exist !!" % path)
                for line in stdout.splitlines():
                    if line == run_nbr[0:3]:  #We have found the 1st 3 digits of our run number
                        path1 = path + "/" + line + "/" + run_nbr[3:6]
                        ls_command = runCommand("ls " + path1)
                        stdout2, stderr2 = ls_command.communicate()
                        status2 = ls_command.returncode
                        if status2 != 0:
                            raise IOError("File/path = %s does not exist !!" % path1)
                        else: #We have found the run number!
                            for line2 in stdout2.splitlines():
                                path2 = path1 + "/" + line2
                                ls_command = runCommand("ls " + path2)
                                stdout3, stderr3 = ls_command.communicate()
                                for line3 in stdout3.splitlines():
                                    if ".root" in line3: #found the root files, and thus the full path to them
                                        keep_looking = False
                                        full_path = path2
                                        break
                                if not keep_looking: break
                            break
                    else:
                        paths.append( path + "/" + line )
                paths.remove( path )
            time_out += 1
            if (time_out == max_loop):
                keep_looking = False
        
        if full_path != "":
            ls_command = runCommand("ls " + full_path)
            stdout, stderr = ls_command.communicate()
            status = ls_command.returncode
            if status != 0:
                raise IOError("File/path = %s does not exist !!" % full_path)
            for line in stdout.splitlines():
                if ".root" in line:
                    outfile.write('"root://eoscms.cern.ch/' + full_path + "/" + line + '"' +',\n')


outfile.write("]")
