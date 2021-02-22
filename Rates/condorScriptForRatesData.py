import time
import datetime
import os
import sys
from aux import runCommand

MYDIR=os.getcwd()  #set MYDIR to the current working directory
#folder = '/store/group/dpg_trigger/comm_trigger/TriggerStudiesGroup/STEAM/Summer16_FlatPU28to62/HLTRates_v4p2_V2_1p25e34_MC_2017feb09J'

#get input options
from optparse import OptionParser
parser=OptionParser()
parser.add_option("-j","--json",dest="jsonFile",type="str",default="nojson",help="text FILE with the LS range in json format",metavar="FILE")
parser.add_option("-e","--env",dest="cmsEnv",type="str",default="noenv",help="DIRECTORY where the top of a CMSSW release can be found : /bla/bla/CMSSW_X_X/src",metavar="DIRECTORY")
parser.add_option("-i","--infiles",dest="inputFilesDir",type="str",default="no",help="In case you want to rerun the make_filesInput.py script, specify the DIR where the input files are located",metavar="DIR")
parser.add_option("-f","--filetype",dest="fileType",type="str",default="custom",help="ARG='custom' (default option) or 'RAW', use 'custom' if you're running on STEAM-made files, 'RAW' if you're running on raw data",metavar="ARG")
parser.add_option("-n",dest="nPerJob",type="int",default=5,help="NUMBER of files processed per job",metavar="NUMBER")
parser.add_option("-q","--flavour",dest="jobFlavour",type="str",default="workday",help="job FLAVOUR",metavar="FLAVOUR")
parser.add_option("-m","--maps",dest="maps",type="str",default="nomaps",help="ARG='nomaps' (default option, don't use maps to get dataste/groups/etc. rates), 'somemaps' (get dataset/groups/etc. rates but with no study of dataset merging), 'allmaps' (get dataset/groups/etc. rates and also study dataset merging)",metavar="ARG")

opts, args = parser.parse_args()


error_text = '\n\nError: wrong <json>=%s or <CMSSWrel>=%s inputs\n' %(opts.jsonFile, opts.cmsEnv)
help_text = '\npython condorScriptForRates.py -j <json> -e <CMSSWrel> -i <infilesDir> -f <filetype> -n <nPerJob> -q <jobFlavour> -m <merging>'
help_text += '\n<json> (mandatory argument) = text file with the LS range in json format'
help_text += '\n<CMSSWrel> (mandatory) = directory where the top of a CMSSW release is located'
help_text += '\n<infilesDir> (optional) = directory where the input root files are located (default = will take whatever is in the filesInputData.py file)'
help_text += '\n<filetype> (optional) = "custom" (default option) or "RAW" or "L1Accept"'
help_text += '\n<nPerJob> (optional) = number of files processed per condor job (default=5)'
help_text += '\n<flavour> (optional) = job flavour (default=workday)\n'
help_text += '\n<maps> (optional) = "nomaps" (default option) or "somemaps" or "allmaps""\n'

if opts.jsonFile == "nojson" or opts.cmsEnv == "noenv":
    print error_text
    print help_text
    sys.exit(2)

print 'json = %s'%opts.jsonFile
print 'CMSSWrel = %s'%opts.cmsEnv
print 'file type = %s'%opts.fileType
print 'job flavour = %s'%opts.jobFlavour


#make directories for the jobs
try:
    os.system('rm -rf Jobs')
    os.system('mkdir Jobs')
    os.system('mkdir Jobs/sub_raw')
except:
    print "err!"
    pass


#file to submit new jobs
sub_total = open("sub_total.jobb","w")
#remove old job outputs (this removal will only happen when/if you execute ./sub_total.jobb)
sub_total.write("rm Results/Data/Raw/*/*.csv\n")
sub_total.write("rm Results/Data/Raw/*/*.root\n")


if opts.inputFilesDir != "no":  #if the option for the input files directory isn't set to its default value (the user entered a directory)
    print 'Making a copy of the old filesInputData.py : filesInputData_old.py'
    os.system('cp filesInputData.py filesInputData_old.py')  #copy the old files input data so that the user still has access to it in case they used the wrong option
    print 'Making a new filesInputData.py with input root files from %s'%opts.inputFilesDir
    os.system('python make_ratesFilesInputData.py -i %s'%opts.inputFilesDir)  #automated creation of lists of input files
else:   #if no directory was specified, use the already existing list of input files
    print 'Taking default input files (from filesInputData.py)' 
from filesInputData import fileInputNames

#Estimate total number of jobs
nJobs = len(fileInputNames) // opts.nPerJob
if nJobs == 0: nJobs = 1
print 'files processed per job = %s, total number of jobs = %s'%(opts.nPerJob, nJobs)

i=0
k=0
loop_mark = opts.nPerJob
tmp_text='#!/bin/sh\n'  #mandatory text at the beginning of the job to explain to condor that this is a *.sh script (shell script)
#make job scripts
for infile in fileInputNames:

    #block for "basic" job, running over only 1 root file
    tmp_jobname="sub_%s.sh"%(str(i))   #give unique name to job file
    tmp_job=open(MYDIR+'/Jobs/sub_raw/'+tmp_jobname,'w')  #create job file (MYDIR = current working dir)
    tmp_job.write("cd %s\n"%(MYDIR))
    tmp_job.write("cd %s\n"%(opts.cmsEnv))   #go to the top of the CMS release specified in the option
    tmp_job.write("eval `scramv1 runtime -sh`\n")  #this is how you write the "cmsenv" command for a condor job
    tmp_job.write("cd -\n")
    #now run the trigger count script
    #note that this is happening in YOUR SteamRatesEdmWorkflow/Rates dir rather than in a temporary dir as for Step 1
    #this works because outputs files are all given unique names
    tmp_job.write("python triggerCountsFromTriggerResults.py -i %s -j %s -s %s -f %s -m %s\n"%(infile, opts.jsonFile, str(i), opts.fileType, opts.maps))  
    tmp_job.write("\npython handleFileTransfer.py -d %s -s %s"%(MYDIR, str(i)))  #transfer files to a better, more organized location
    tmp_job.close()
    tmp_job_dir = MYDIR+'/Jobs/sub_raw/'+tmp_jobname
    os.system("chmod +x %s"%(tmp_job_dir))  #change access rights to the script so condor can use it
    #END block for "basic" job


    #block for the higher-level job, constituted of multiple "basic" jobs, depending on the option used
    tmp_text = tmp_text + tmp_job_dir + "\n" #add the name of one "basic" job
    k+=1
    if k==loop_mark or i==len(fileInputNames)-1: 
        #we only enter this blcok once the number of "basic" jobs in tmp_text equals the number of files per job set in the options
        k=0
        Tjobsname = "sub_%s.sh"%i
        Tjob_dir = '%s/Jobs/Job_%s/'%(MYDIR, str(i))
        os.system("mkdir %s"%Tjob_dir)  #create dir with unique name for each higher-level job
        #this dir will also be used to house the errors/outputs/log of the condor job
        Tjob = open(Tjob_dir+Tjobsname,"w")  #open file for higher level job
        Tjob.write("%s"%(tmp_text))        #write tmp_text, which has been progressively filled with "basic" jobs, into the higher-level job
        os.system("chmod +x %s"%(Tjob_dir))  #change access rights so condor can run the job
        tmp_text='#!/bin/sh\n'            #start text for the next higher-level job

    i+=1

#master file for sending the condor jobs, similar to the one used in Step 1
condor_str = "executable = $(filename)\n"
condor_str += "arguments = $Fp(filename) $(ClusterID) $(ProcId)\n"
condor_str += "output = $Fp(filename)counts.stdout\n"
condor_str += "error = $Fp(filename)counts.stderr\n"
condor_str += "log = $Fp(filename)counts.log\n"
condor_str += '+JobFlavour = "%s"\n'%opts.jobFlavour
condor_str += "queue filename matching ("+MYDIR+"/Jobs/Job_*/*.sh)"
condor_name = MYDIR+"/condor_cluster.sub"
condor_file = open(condor_name, "w")
condor_file.write(condor_str)
sub_total.write("condor_submit %s\n"%condor_name)  #the "sub_total" file first removes the old job outputs (see many lines above) and then submits the freshly created condor jobs (see this line)
os.system("chmod +x sub_total.jobb")
