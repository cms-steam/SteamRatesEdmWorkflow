'''
This file allows you to merge the outputs of the batch jobs, and get the trigger rates. You may also draw some figures if you wish.
The resulting .csv and .root files will be located in the 'Results' directory. The figures will be in the 'Figures' directory.
'''

import os
import sys


'''
--------------------------OPTIONS TO BE FILLED OUT-----------------------------------------
'''
#Write the average instant lumi of the json you ran over
#Units: 1e34 /cm^2/s
#lumi_in = 2.18 #average PU = 63.2
lumi_in = 1.05 # for Run 401844 with 1200 bunches and PU = 64.8
# lumi_in = 0.24 #average PU = 7

#Write the TARGET lumi for which you wish to calculate rates
#Units: 1e34 /cm^2/s
#lumi_target = 2.1
lumi_target = 1.05 # for Run 401844 with 1200 bunches and PU = 64.8
# lumi_target = 0.24 #average PU = 7

#Write the HLT prescale used in the json you ran over
#hlt_ps = 1760 # Run 398183 EphemeralHLTPhysics 
# hlt_ps = 213848  # Run 398183 EphemeralZeroBias (26731*8)
# hlt_ps = 16518  # Run 398683 SpecialZeroBias (2753*6)
hlt_ps = 480 # for Run 401844 with 1200 bunches and PU = 64.8

#Maps option should be the same one you use to make the batch jobs
maps = "nomaps"
#maps = "somemaps"
#maps = "allmaps"

#Do you wish to draw the figures? If you have a slow connection, drawing might take a while
#his boolean will be set to False if you used the "nomaps" option
#makeFigures = False
makeFigures = False

#Do you wish to take input files from an unusual location (different from the default one)?
#If you do, set the following boolean to True
diffLoc = False
#If yes, please specify the directory where the job outputs are located
files_dir = "Results/Data/Raw"
'''
--------------------------OPTIONS TO BE FILLED OUT-----------------------------------------
'''


#run the script
command = ""
if diffLoc:
    command = "python3 mergeOutputs.py -l %s -t %s -p %s -d %s" %(lumi_in, lumi_target, hlt_ps, files_dir)
else:
    command = "python3 mergeOutputs.py -l %s -t %s -p %s" %(lumi_in, lumi_target, hlt_ps)

command += " -m %s" %maps

if maps =="nomaps":
    makeFigures = False

if makeFigures:
    command += " -f"

os.system(command)
if makeFigures: 
    comm2 = "python3 Draw.py"
    if maps == "allmaps": comm2 += " -m yes"
    os.system(comm2)






