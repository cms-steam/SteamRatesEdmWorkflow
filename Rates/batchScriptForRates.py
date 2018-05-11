import time
import datetime
import os
import sys
from aux import runCommand

MYDIR=os.getcwd()
#folder = '/store/group/dpg_trigger/comm_trigger/TriggerStudiesGroup/STEAM/Summer16_FlatPU28to62/HLTRates_v4p2_V2_1p25e34_MC_2017feb09J'

#get inputs
from optparse import OptionParser
parser=OptionParser()
parser.add_option("-j","--json",dest="jsonFile",type="str",default="nojson",help="text FILE with the LS range in json format",metavar="FILE")
parser.add_option("-e","--env",dest="cmsEnv",type="str",default="noenv",help="DIRECTORY where the top of a CMSSW release can be found : /bla/bla/CMSSW_X_X/src",metavar="DIRECTORY")
parser.add_option("-i","--infiles",dest="inputFilesDir",type="str",default="no",help="In case you want to rerun the make_filesInput.py script, specify the DIR where the input files are located",metavar="DIR")
parser.add_option("-f","--filetype",dest="fileType",type="str",default="custom",help="ARG='custom' (default option) or 'RAW', use 'custom' if you're running on STEAM-made files, 'RAW' if you're running on raw data",metavar="ARG")
parser.add_option("-n",dest="nPerJob",type="int",default=5,help="NUMBER of files processed per batch job",metavar="NUMBER")
parser.add_option("-q","--queue",dest="batchQueue",type="str",default="8nh",help="batch QUEUE",metavar="QUEUE")
parser.add_option("-m","--maps",dest="maps",type="str",default="nomaps",help="ARG='nomaps' (default option, don't use maps to get dataste/groups/etc. rates), 'somemaps' (get dat
aset/groups/etc. rates but with no study of dataset merging), 'allmaps' (get dataset/groups/etc. rates and also study dataset merging)",metavar="ARG")

opts, args = parser.parse_args()


error_text = '\n\nError: wrong <json>=%s or <CMSSWrel>=%s inputs\n' %(opts.jsonFile, opts.cmsEnv)
help_text = '\npython batchScriptForRates.py -j <json> -e <CMSSWrel> -i <infilesDir> -f <filetype> -n <nPerJob> -q <batchQueue> -m <merging>'
help_text += '\n<json> (mandatory argument) = text file with the LS range in json format'
help_text += '\n<CMSSWrel> (mandatory) = directory where the top of a CMSSW release is located'
help_text += '\n<infilesDir> (optional) = directory where the input root files are located (default = will take whatever is in the filesInput.py file)'
help_text += '\n<filetype> (optional) = "custom" (default option) or "RAW"'
help_text += '\n<nPerJob> (optional) = number of files processed per batch job (default=5)'
help_text += '\n<nPerJob> (optional) = batch queue (default=8nh)\n'
help_text += '\n<maps> (optional) = "nomaps" (default option) or "somemaps" or "allmaps""\n'

if opts.jsonFile == "nojson" or opts.cmsEnv == "noenv":
    print error_text
    print help_text
    sys.exit(2)

print 'json = %s'%opts.jsonFile
print 'CMSSWrel = %s'%opts.cmsEnv
print 'file type = %s'%opts.fileType
print 'files processed per job = %s'%opts.nPerJob
print 'batch queue = %s'%opts.batchQueue


#make directories for the jobs
try:
    os.system('rm -r Jobs')
    os.system('mkdir Jobs')
    os.system('mkdir Jobs/sub_err')
    os.system('mkdir Jobs/sub_out')
    os.system('mkdir Jobs/sub_job')
    os.system('mkdir Jobs/sub_job2')
except:
    print "err!"
    pass


sub_total = open("sub_total.jobb","w")
sub_total.write("rm Results/Raw/*/*.csv\n")
sub_total.write("rm Results/Raw/*/*.root\n")


if opts.inputFilesDir != "no":
    print 'Making a copy of the old filesInput.py : filesInput_old.py'
    os.system('cp filesInput.py filesInput_old.py')
    print 'Making a new filesInput.py with input root files from %s'%opts.inputFilesDir
    os.system('python make_ratesFilesInput.py -i %s'%opts.inputFilesDir)
else:
    print 'Taking default input files (from filesInput.py)'
from filesInput import fileInputNames

k = 0
pre_k = 0
i=0
loop_mark = opts.nPerJob
tmp_text=''
#make job scripts
for infile in fileInputNames:
    #print 'total: %d/%d  ;  %.1f %% processed '%(j,my_sum,(100*float(j)/float(my_sum)))

    tmp_jobname="sub_%s.jobb"%(str(i))
    tmp_job=open(MYDIR+'/Jobs/sub_job/'+tmp_jobname,'w')
    tmp_job.write("curr_dir=%s\n"%(MYDIR))
    tmp_job.write("cd %s\n"%(MYDIR))
    tmp_job.write("cd %s\n"%(opts.cmsEnv))
    tmp_job.write("eval `scramv1 runtime -sh`\n")
    tmp_job.write("cd -\n")
    tmp_job.write("python triggerRatesFromTriggerResults.py -i %s -j %s -s %s -f %s -m %s\n"%(infile, opts.jsonFile, str(k), opts.fileType, opts.newDataset))
    tmp_job.write("\n")
    tmp_job.close()
    tmp_job_dir = MYDIR+'/Jobs/sub_job/'+tmp_jobname
    os.system("chmod +x %s"%(tmp_job_dir))


    k+=1
    tmp_text = tmp_text + tmp_job_dir + "\n"
    i+=1
    if k % loop_mark == 0 or i==len(fileInputNames):
        Tjobsname = "sub_%s_%s.jobb"%(pre_k, k)
        Tjob_dir = MYDIR+'/Jobs/sub_job2/'+Tjobsname
        Tjob = open(Tjob_dir,"w")
        Tjob.write("%s"%(tmp_text))
        os.system("chmod +x %s"%(Tjob_dir))
        sub_str = "bsub -q %s -eo Jobs/sub_err/err_%s.dat -oo Jobs/sub_out/out_%s.dat %s"%(opts.batchQueue, k, k, Tjob_dir)
        local_str = "%s"%(Tjob_dir)
        #os.system(sub_str)
        sub_total.write("%s\n"%(sub_str))
        pre_k = k
        tmp_text = ''
os.system("chmod +x sub_total.jobb")


