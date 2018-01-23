#from listsOfRawOutputs import scaleFactor
import os
from aux import runCommand
from aux import mergeNames

masterDic = {}


#MYDIR=os.getcwd()
files_dir = "Results/Raw"
#files_dir = "ResultsCheng/Results/ResultsPS6/Raw/Raw"


for name in mergeNames:
    key = name+".csv"
    masterDic[key] = []

    total_dir = files_dir + "/" + mergeNames[name]
    ls_command = runCommand("ls " + total_dir )
    stdout, stderr = ls_command.communicate()
    for line in stdout.splitlines():
        masterDic[key].append(total_dir + "/" + line)
        

rootFiles = []
total_dir = files_dir + "/Root"
ls_command = runCommand("ls " + total_dir)
stdout, stderr = ls_command.communicate()
for line in stdout.splitlines():
    rootFiles.append(total_dir + "/" + line)


globalFiles = []
total_dir = files_dir + "/Global"
ls_command = runCommand("ls " + total_dir)
stdout, stderr = ls_command.communicate()
for line in stdout.splitlines():
    globalFiles.append(total_dir + "/" + line)






