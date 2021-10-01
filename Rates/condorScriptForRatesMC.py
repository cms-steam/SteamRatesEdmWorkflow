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
parser.add_option("-e","--env",dest="cmsEnv",type="str",default="noenv",help="DIRECTORY where the top of a CMSSW release can be found : /bla/bla/CMSSW_X_X/src",metavar="DIRECTORY")
parser.add_option("-i","--infiles",dest="inputFilesDir",type="str",default="no",help="In case you want to rerun the make_filesInput.py script, specify the DIR where the input files are located",metavar="DIR")
parser.add_option("-f","--filetype",dest="fileType",type="str",default="custom",help="ARG='custom' (default option) or 'RAW', use 'custom' if you're running on STEAM-made files, 'RAW' if you're running on raw data",metavar="ARG")
parser.add_option("-n",dest="nPerJob",type="int",default=5,help="NUMBER of files processed per job",metavar="NUMBER")
parser.add_option("-q","--flavour",dest="jobFlavour",type="str",default="workday",help="job FLAVOUR",metavar="FLAVOUR")
parser.add_option("-m","--maps",dest="maps",type="str",default="nomaps",help="ARG='nomaps' (default option, don't use maps to get dataste/groups/etc. rates), 'somemaps' (get dataset/groups/etc. rates but with no study of dataset merging), 'allmaps' (get dataset/groups/etc. rates and also study dataset merging)",metavar="ARG")

opts, args = parser.parse_args()


error_text = '\n\nError: wrong <CMSSWrel>=%s input\n' %opts.cmsEnv
help_text = '\npython batchScriptForRates.py -e <CMSSWrel> -i <infilesDir> -f <filetype> -n <nPerJob> -q <jobFlavour> -m <merging>'
help_text += '\n<CMSSWrel> (mandatory) = directory where the top of a CMSSW release is located'
help_text += '\n<infilesDir> (optional) = directory where the input root files are located (default = will take whatever is in the filesInputMC.py file)'
help_text += '\n<filetype> (optional) = "custom" (default option) or "RAW"'
help_text += '\n<nPerJob> (optional) = number of files processed per batch job (default=5)'
help_text += '\n<flavour> (optional) = job flavour (default=workday)\n'
help_text += '\n<maps> (optional) = "nomaps" (default option) or "somemaps" or "allmaps""\n'

if opts.cmsEnv == "noenv":
    print(error_text)
    print(help_text)
    sys.exit(2)

print('CMSSWrel = %s'%opts.cmsEnv)
print('file type = %s'%opts.fileType)
print('job flavour = %s'%opts.jobFlavour)


#make directories for the jobs
try:
    os.system('rm -rf Jobs')
    os.system('mkdir Jobs')
except:
    print("err!")
    pass


sub_total = open("sub_total.jobb","w")
sub_total.write("rm Results/MC/*/Raw/*/*.csv\n")
sub_total.write("rm Results/MC/*/Raw/*/*.root\n")


if opts.inputFilesDir != "no":
    print('Making a copy of the old filesInputMC.py : filesInputMC_old.py')
    os.system('cp filesInputMC.py filesInputMC_old.py')
    print('Making a new filesInputMC.py with input root files from %s'%opts.inputFilesDir)
    os.system('python make_ratesFilesInputMC.py -i %s'%opts.inputFilesDir)
else:
    print('Taking default input files (from filesInputMC.py)')
from filesInputMC import datasetFilesMap


for dataset in list(datasetFilesMap.keys()):
    os.system('mkdir Jobs/%s'%dataset)
    os.system('mkdir Jobs/%s/sub_raw'%dataset)
    filesList = datasetFilesMap[dataset]
    nJobs = len(filesList) // opts.nPerJob
    if nJobs == 0: nJobs = 1
    print('\n-------------------------------------------')
    print('%s files in dataset %s'%(len(filesList), dataset))
    print('files processed per job = %s, number of jobs for this dataset = %s'%(opts.nPerJob, nJobs))

    i=0
    k=0
    loop_mark = opts.nPerJob
    tmp_text='#!/bin/sh\n'
    #make job scripts
    for infile in filesList:
        #print 'total: %d/%d  ;  %.1f %% processed '%(j,my_sum,(100*float(j)/float(my_sum)))
    
        tmp_jobname="sub_%s.sh"%(str(i))
        tmp_job=open(MYDIR+'/Jobs/%s/sub_raw/%s'%(dataset, tmp_jobname),'w')
        tmp_job.write("cd %s\n"%(MYDIR))
        tmp_job.write("cd %s\n"%(opts.cmsEnv))
        tmp_job.write("eval `scramv1 runtime -sh`\n")
        tmp_job.write("cd -\n")
        tmp_job.write("python triggerCountsFromTriggerResults.py -i %s -s %s -j %s -f %s -m %s\n"%(infile, str(i), dataset, opts.fileType, opts.maps))
        tmp_job.write("\npython handleFileTransfer.py -d %s -m %s -s %s"%(MYDIR, dataset, str(i)))
        tmp_job.close()
        tmp_job_dir = MYDIR+'/Jobs/%s/sub_raw/%s'%(dataset, tmp_jobname)
        os.system("chmod +x %s"%(tmp_job_dir))
    
    
        tmp_text = tmp_text + tmp_job_dir + "\n"
        k+=1
        if k==loop_mark or i==len(filesList)-1:
            k=0
            Tjobsname = "sub_%s.sh"%i
            Tjob_dir = '%s/Jobs/%s/Job_%s/'%(MYDIR, dataset, str(i))
            os.system("mkdir %s"%Tjob_dir)
            Tjob = open(Tjob_dir+Tjobsname,"w")
            Tjob.write("%s"%(tmp_text))
            tmp_text='#!/bin/sh\n'
            os.system("chmod +x %s"%(Tjob_dir))
        i+=1
    

condor_str = "executable = $(filename)\n"
condor_str += "arguments = $Fp(filename) $(ClusterID) $(ProcId)\n"
condor_str += "output = $Fp(filename)counts.stdout\n"
condor_str += "error = $Fp(filename)counts.stderr\n"
condor_str += "log = $Fp(filename)counts.log\n"
condor_str += '+JobFlavour = "%s"\n'%opts.jobFlavour
condor_str += "queue filename matching ("+MYDIR+"/Jobs/*/Job_*/*.sh)"
condor_name = MYDIR+"/condor_cluster.sub"
condor_file = open(condor_name, "w")
condor_file.write(condor_str)
sub_total.write("condor_submit %s\n"%condor_name)
os.system("chmod +x sub_total.jobb")
