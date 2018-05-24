'''
This file allows you to automatically compare the physics path rates results you obtained with the STEAM tools with WBM rates.
A csv file containing WBM rates and PS will be created, called WBM.csv
A comparison csv file will also be created: Results/comparison.csv
'''

import os
import sys


'''
--------------------------OPTIONS TO BE FILLED OUT-----------------------------------------
'''
#Which run number do you want to use to get the WBM rates? Which LS range?
run = 316505
LS_begin = 61
LS_end = 86

#What's the name of the PS column used in that LS range?
ps_column = "2.0e34"

#What lumi scaling do you want to use on the WBM rates?
scaling =  2.0e34/1.69e34

#We need cookies to get access to WBM from the terminal. Where do you want to store the cookies?
cookie_path = "/tmp/USERNAME_sso"

#Do you want to get the L1 seeds from the Menu_HLT.py file? (recommended) Set this boolean to True.
#Otherwise, the L1 seeds will be taken from WBM, and there is a known bug where some of the seeds fail to show up.
bFromUser = True

'''
--------------------------OPTIONS TO BE FILLED OUT-----------------------------------------
'''


#run the script
command = "python makeWBMCSV.py -p %s -r %s -l %s-%s -s %s -c %s" %(ps_column, run, LS_begin, LS_end, scaling, cookie_path)
if bFromUser:
    command += "-m fromUser"

os.system(command)
os.system("python compareWBM.py")






