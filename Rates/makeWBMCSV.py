#import ROOT
import math
import os
import sys
import csv

#auxiliary function to get the minimum L1 PS
def minPS(L1table, mmap):
    default_value = -10
    min_PS = int(default_value)
    for seed in L1table:
        seed = seed.strip(" ()")
        if "AND" in seed:
            sseed = seed.split(" AND ")
            seed = sseed[0].strip(" ()")
        if not seed in list(mmap.keys()):
            #print "\n\nWARNING:\n L1 seed '%s' is not in the L1 PS map.\n\n\n" %seed
            continue 
        if (mmap[seed] > 0) and ((min_PS == default_value) or (mmap[seed] < min_PS)):
            min_PS = int(mmap[seed])
    if min_PS == default_value: min_PS = 0
    return min_PS


from optparse import OptionParser
parser=OptionParser()
parser.add_option("-p","--prescale",dest="PS",type="str",default="Index1",help="PRESCALE column which was used when the WBM data was taken",metavar="PRESCALE")
parser.add_option("-r","--run",dest="RUN",type="int",default=-1,help="Run NUMBER",metavar="NUMBER")
parser.add_option("-l","--ls",dest="LS",type="str",default="noLS",help="starting and final lumi sections, separated by '-', LS_begin-LS_end")
parser.add_option("-s","--scaling",dest="scaleFactor",type="str",default="1",help="luminosity scaling you want to apply, you can write it as ratio ('2.0e34/1.12e33') or as a number ('1.6'). '1' is the default scaling")
parser.add_option("-m","--maps",dest="maps",type="str",default="fromWBM",help="Make the code run with a user-provided map to get the L1 seed? ('fromUser' option) Or trust WBM? ('fromWBM', default option)")
parser.add_option("-c","--cookie",dest="cookie",type="str",default="nocookie",help="PATH to the temporary directory where you want to store your CERN cookie\nTemplate: /tmp/USERNAME_sso",metavar="PATH")

opts, args = parser.parse_args()

if (not '-' in opts.LS) or (opts.RUN < 0):
    print("Error in LS or run number inputs. Try 'python makeWBMCSV.py --help' for help.")
    sys.exit(2)

sep_pos = int(opts.LS.find('-'))
LS_begin = opts.LS[:sep_pos]
LS_end = opts.LS[sep_pos+1:]
print("LS=", LS_begin, LS_end)

if opts.cookie == "nocookie":
    print("\nYou didn't provide a path to your cookies. I'm assuming you have already set up your cookies properly.\nIf the script crashes, please use the --cookie option. Try 'python makeWBMCSV.py --help' for help.\n")
else:
    print("\nGetting cookies... If this doesn't work, it's probably because you set up a CMS environment ('cmsenv'). If so, try again with a new terminal.\n...\n")
    os.environ["SSO_COOKIE"] = opts.cookie
    os.system("cern-get-sso-cookie --krb -r -u https://cmswbm.cern.ch/cmsdb/servlet -o $SSO_COOKIE")

sf = 1.0
if '/' in opts.scaleFactor:
    division_terms = opts.scaleFactor.split('/')
    sf = float(division_terms[0])/float(division_terms[1])
else:
    sf = float(opts.scaleFactor)
print("Rates will be scaled by your input %s = %s" %(opts.scaleFactor, sf))


from cernSSOWebParser2 import parseURLTables
#https://cmswbm.cern.ch/cmsdb/servlet/HLTSummary?fromLS=14&toLS=17&RUN=306154

#Make a map linking L1 seeds to their PS
map_L1 = {}
url="https://cmswbm.cern.ch/cmsdb/servlet/PrescaleSets?RUN=%s" %opts.RUN
tables=parseURLTables(url)
ps_col_L1 = -1
for i in range(0, len(tables)):
    ps_column = -1
    ps_string_tobefound = opts.PS
    L1seed_column = -1
    L1seed_string_tobefound = "L1 Algo Name"
    for k in range(0, len(tables[i][0])):
        if ps_string_tobefound in tables[i][0][k]:
            ps_column = k
        if L1seed_string_tobefound in  tables[i][0][k]:
            L1seed_column = k
    if ps_column == -1 or L1seed_column == -1: continue
    ps_col_L1 = ps_column
    for j in range(1, len(tables[i])):
        L1seed = str(tables[i][j][L1seed_column])
        L1ps = int(tables[i][j][ps_column])
        map_L1[L1seed] = L1ps
    break
            

#Make a map linking HLT paths to their HLT PS *and* their L1 PS
map_PS = {}

#To get the HLT PS url, we need first to find the hlt key
url="https://cmswbm.cern.ch/cmsdb/servlet/RunSummary?RUN=%s&DB=default" %opts.RUN
tables=parseURLTables(url)
l1_hlt_mode=""
hlt_key=""
for i in range(0, len(tables)):
    index_l1_hlt_mode = -1
    index_hlt_key = -1
    label_to_find_l1_hlt_mode = "TRIGGER_MODE"
    label_to_find_hlt_key = "HLT_KEY"
    if label_to_find_l1_hlt_mode in tables[i][0]:
        index_l1_hlt_mode = tables[i][0].index(label_to_find_l1_hlt_mode)
    if label_to_find_hlt_key in tables[i][0]:
        index_hlt_key = tables[i][0].index(label_to_find_hlt_key)
    if index_l1_hlt_mode == -1 or index_hlt_key == -1: continue
    l1_hlt_mode=tables[i][1][index_l1_hlt_mode] #this'll be useful right away
    hlt_key=tables[i][1][index_hlt_key] #this'll be useful later
    break
url="https://cmswbm.cern.ch/cmsdb/servlet/TriggerMode?KEY=%s" % (l1_hlt_mode)
tables=parseURLTables(url)

#We got the tables, let's find the HLT prescales and L1 seeds
ps_col_HLT = -1
for i in range(0, len(tables)):
    ps_column = -1
    path_column = -1
    path_string_tobefound = "HLT Path Name"
    L1seeds_column = -1
    L1seeds_string_tobefound = "L1 Prerequisite"
    for k in range(0, len(tables[i][0])):
        if opts.PS in tables[i][0][k]:
            ps_column = k
        if path_string_tobefound in tables[i][0][k]:
            path_column = k
        if L1seeds_string_tobefound in  tables[i][0][k]:
            L1seeds_column = k
    if ps_column == -1 or path_column == -1 or L1seeds_column == -1: continue
    ps_col_HLT = ps_column

    for j in range(1, len(tables[i])):
        HLTpath = str(tables[i][j][path_column])
        HLTps = int(tables[i][j][ps_column])
        L1seeds_list = str(tables[i][j][L1seeds_column])

        HLTKey = HLTpath.strip("0123456789)(")
        HLTKey = HLTKey.strip(" ")
        HLTKey = HLTKey.strip("0123456789")


        #Find L1 PS using the L1 prescale map
        L1ps = -1
        cleanedstr = ''
        if opts.maps == "fromWBM":
            cleanedstr = L1seeds_list.lstrip(" ")
        elif opts.maps == "fromUser":
            from Menu_HLT import seedMap
            for key in list(seedMap.keys()):
                if HLTKey in key:
                    cleanedstr = seedMap[key]
        L1seeds = cleanedstr.split(' OR ')
        L1ps = minPS(L1seeds, map_L1)
        
        map_PS[HLTKey] = [L1ps, HLTps]
    break


if ps_col_L1 != ps_col_HLT:
    print("!!!!WARNING!!!!\nDifferent PS column at L1 (%s) and HLT (%s)\n!!!!!!!!!!!!!!!\n" %(ps_col_L1, ps_col_HLT))
ps_out = open('pscolumn.txt', 'w')
ps_out.write(str(ps_col_L1))
ps_out.close()

file_out = open('WBM.csv', 'w')
file_out.write('Paths, L1 PS, HLT PS, Counts, Rates (Hz)\n')

#We get the HLT rates from WBM, using the HLT key we got before
url="https://cmswbm.cern.ch/cmsdb/servlet/HLTSummary?fromLS=%s&toLS=%s&RUN=%s&NAME=%s" %(LS_begin, LS_end, opts.RUN, hlt_key)
tables=parseURLTables(url)
for i in range(0, len(tables)):
    look_in_line = 1
    path_column = -1
    path_string_tobefound = "Name"
    count_column = -1
    count_string_tobefound = "PAccept"
    rate_column = -1
    rate_string_tobefound = "RateHz"
    if len(tables[i]) <= look_in_line: continue
    for k in range(0, len(tables[i][look_in_line])):
        if path_string_tobefound in tables[i][look_in_line][k]:
            path_column = k
        if count_string_tobefound in tables[i][look_in_line][k]:
            count_column = k
        if rate_string_tobefound in tables[i][look_in_line][k]:
            rate_column = k
    if path_column == -1 or rate_column == -1 or count_column == -1: continue

    for j in range(look_in_line+1, len(tables[i])):
        HLTKey = str(tables[i][j][path_column]).rstrip("0123456789 )(")
        HLTKey = HLTKey.strip(" ")
        if not HLTKey in list(map_PS.keys()): continue
        count = str(tables[i][j][count_column]).strip(" ")
        count = count.replace(",", "")
        rate = str(tables[i][j][rate_column]).strip(" ")
        rate = rate.replace(",", "")
        rate = str(float(rate)*sf)
        file_out.write( '%s, %s, %s, %s, %s\n'%(HLTKey, map_PS[HLTKey][0], map_PS[HLTKey][1], count, rate) )
    break

file_out.close()
