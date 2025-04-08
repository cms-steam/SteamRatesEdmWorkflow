'''
This file allows you to merge the outputs of the batch jobs, and get the trigger rates. You may also draw some figures if you wish.
The resulting .csv and .root files will be located in the 'Results' directory. The figures will be in the 'Figures' directory.
'''

import os
import sys


'''
--------------------------OPTIONS TO BE FILLED OUT-----------------------------------------
'''
#Write the TARGET lumi for which you wish to calculate rates
#Units: 10e34 /cm^2/s
lumi_target = 2.2


#Maps option should be the same one you use to make the batch jobs
#maps = "nomaps"
maps = "somemaps"
#maps = "allmaps"

#Do you wish to draw the figures? If you have a slow connection, drawing might take a while
#This boolean will be set to False if you used the "nomaps" option
makeFigures = False
#makeFigures = True

#Do you wish to take input files from an unusual location (different from the default one)?
#If you do, set the following boolean to True                                              
diffLoc = False
#If yes, please specify the directory where the job outputs are located                    
files_dir = "Results/Raw"

'''
--------------------------OPTIONS TO BE FILLED OUT-----------------------------------------
'''



#run the script
command = ""
if diffLoc:
    command = "python3 prepareMergeOutputsMC.py -t %s -d %s" %(lumi_target, files_dir)
else:
    command = "python3 prepareMergeOutputsMC.py -t %s" %lumi_target

command += " -m %s" %maps

if maps =="nomaps":
    makeFigures = False

if makeFigures: command += " -f"

os.system(command)






