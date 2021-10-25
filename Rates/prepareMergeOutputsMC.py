'''
                       Prepare the merging jobs for MC
''' 
import math
import os
import sys

from aux import runCommand

from optparse import OptionParser
parser=OptionParser()
parser.set_defaults(figures=False)
parser.add_option("-t","--lumitarget",dest="lumiTarget",type="float",default=-1,help="VALUE corresponding to the target instant lumi for which you wish to calculate your rates",metavar="VALUE")
parser.add_option("-d","--dir",dest="inDir",type="str",default="Results/MC",help="DIR where the output of the batch jobs are located",metavar="DIR")
parser.add_option("-m","--maps",dest="maps",type="str",default="nomaps",help="ARG='nomaps' (default option, don't use maps to get dataste/groups/etc. rates), 'somemaps' (get dataste/groups/etc. rates but with no study of dataset merging), 'allmaps' (get dataste/groups/etc. rates and also study dataset merging)",metavar="ARG")
parser.add_option("-f", action="store_true", dest="figures",help="Add this option for figures")
opts, args = parser.parse_args()

error_text = '\nError: wrong inputs\n'
help_text = '\npython3 prepareMergeOutputsMC.py -t <lumitarget> -d <dir> -m <merging>\n -f'
help_text += '(mandatory) <lumitarget> = VALUE corresponding to the target instant lumi for which you wish to calculate your rates\n'
help_text += '(optional) <dir> = DIR where the output of the batch jobs are located'
help_text += '\n(optional) <maps> = "nomaps" (default option, use none of the maps), "somemaps" (use all maps except those related to dataset merging), "allmaps" (use all maps, including dataset merging)\n'
help_text += '\n(optional) -f  : Adding this option merges the root files which are used to produce trigger-dataset and dataset-dataset correlation figures. By default root files are NOT merged\n'
if opts.lumiTarget == -1:
    print(error_text)
    print(help_text)
    sys.exit(2)    


#copy MC datasets file here so it can be used
os.system("cp ../MCDatasets/map_MCdatasets_xs.py .")

merge_command="python3 mergeOutputs.py -t %s -m %s" %(opts.lumiTarget, opts.maps)
if opts.figures: merge_command += " -f"

files_dir = opts.inDir
ls_command = runCommand("ls " + files_dir)
stdout, stderr = ls_command.communicate()
for line in stdout.splitlines():
    tmp_merge_command = merge_command + " -d %s/%s/Raw -w %s" %(opts.inDir, line, line)
    os.system(tmp_merge_command)
    if opts.figures: os.system("python3 Draw.py -d %s/%s" %(opts.inDir, line))
