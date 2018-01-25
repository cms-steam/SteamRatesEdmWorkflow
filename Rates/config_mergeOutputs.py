'''
This file allows you to merge the outputs of the batch jobs, and get the trigger rates. You may also draw some figures if you wish.
The resulting .csv and .root files will be located in the 'Results' directory. The figures will be in the 'Figures' directory.
'''

import os
import sys


'''
--------------------------OPTIONS TO BE FILLED OUT-----------------------------------------
'''
#Do you wish to draw the figures? If you have a slow connection, drawing might take a while
makeFigures = True

#Write the average instant lumi of the json you ran over
lumi_in =     1.06e34

#Write the TARGET lumi for which you wish to calculate rates
lumi_target = 1.1e34

#Write the HLT prescale used in the json you ran over
hlt_ps = 10

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
    command = "python mergeOutputs.py -l %s -t %s -p %s -d %s" %(lumi_in, lumi_target, hlt_ps, files_dir)
else:
    command = "python mergeOutputs.py -l %s -t %s -p %s" %(lumi_in, lumi_target, hlt_ps)

os.system(command)
if makeFigures: os.system("python Draw.py")






