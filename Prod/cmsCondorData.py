#!/usr/bin/env python

#Create condor jobs for running over data files

import imp, re, pprint, string

# cms specific
import FWCore.ParameterSet.Config as cms

import time
import datetime
import os
import sys

#determine current directory
MYDIR=os.getcwd()
#folder = '/store/group/dpg_trigger/comm_trigger/TriggerStudiesGroup/STEAM/Summer16_FlatPU28to62/HLTRates_v4p2_V2_1p25e34_MC_2017feb09J'

#get options and arguments
from optparse import OptionParser
parser=OptionParser()

#options
parser.add_option("-n",dest="nPerJob",type="int",default=1,help="NUMBER of files processed per job",metavar="NUMBER")
parser.add_option("-q","--flavour",dest="jobFlavour",type="str",default="workday",help="job FLAVOUR",metavar="FLAVOUR")
parser.add_option("-p","--proxy",dest="proxyPath",type="str",default="noproxy",help="Proxy path")

opts, args = parser.parse_args()


help_text = '\n./cmsCondorData.py <cfgFileName> <CMSSWrel> <remoteDir> -p <proxyPath> -n <nPerJob> -q <jobFlavour>'
help_text += '\n<cfgFileName> (mandatory) = name of your configuration file (e.g. hlt_config.py)'
help_text += '\n<CMSSWrel> (mandatory) = directory where the top of a CMSSW release is located'
help_text += '\n<remoteDir> (mandatory) = directory where the files will be transfered (e.g. on EOS)'
help_text += '\n<proxyPath> (optional) = location of your voms cms proxy. Note: keep your proxy in a private directory.'
help_text += '\n<nPerJob> (optional) = number of files processed per batch job (default=1, recommended)'
help_text += '\n<flavour> (optional) = job flavour (default=workday)\n'


#(mandatory) arguments
cfgFileName = str(args[0])
cmsEnv = str(args[1])
remoteDir = str(args[2])

#Print information about options and arguments used, for the benefit of who's running the script
print 'config file = %s'%cfgFileName
print 'CMSSW release = %s'%cmsEnv
print 'proxy = %s'%opts.proxyPath
print 'remote directory = %s'%remoteDir
print 'job flavour = %s'%opts.jobFlavour


#make directories for the jobs
try:
    os.system('rm -rf Jobs')
    os.system('mkdir Jobs')
except:
    print "err!"
    pass


#create script to submit the jobs
sub_total = open("sub_total.jobb","w")


# load configuration script
handle = open(cfgFileName, 'r')
cfo = imp.load_source("pycfg", cfgFileName, handle)
process = cfo.process  #all info about the HLT you want to test, the job output, and the input files is saved in "process"
handle.close()

# keep track of the original source (process.source will be modified later)
fullSource = process.source.clone()


nJobs = -1

try:   #check if the input files are correctly loaded
    process.source.fileNames
except:
    print 'No input file. Exiting.'
    sys.exit(2)
else:
    print "Number of files in the source:",len(process.source.fileNames), ":"
    pprint.pprint(process.source.fileNames)
   
    #Calculate the number of jobs to be created, then print it to the terminal
    nFiles = len(process.source.fileNames)
    nJobs = nFiles / opts.nPerJob
    if (nJobs!=0 and (nFiles % opts.nPerJob) > 0) or nJobs==0:
        nJobs = nJobs + 1
      
    print "number of jobs to be created: ", nJobs
    




k=0
loop_mark = opts.nPerJob
#make job scripts
#loop over the total number of jobs
for i in range(0, nJobs):

    #create a dedicated directory for each job
    jobDir = MYDIR+'/Jobs/Job_%s/'%str(i) 
    os.system('mkdir %s'%jobDir)

    #name and open file for the condor job
    tmp_jobname="sub_%s.sh"%(str(i))
    tmp_job=open(jobDir+tmp_jobname,'w')

    #start writing in the condor job file
    tmp_job.write("#!/bin/sh\n")  #line telling condor this is a *.sh script
    if opts.proxyPath != "noproxy":  #if you specified a proxy
        #Explain to condor how to use your proxy
        tmp_job.write("export X509_USER_PROXY=$1\n")
        tmp_job.write("voms-proxy-info -all\n")
        tmp_job.write("voms-proxy-info -all -file $1\n")  # '$1' is defined 2 lines up (it's the proxy)
    tmp_job.write("ulimit -v 5000000\n")  #maximum RAM (?) for your job
    tmp_job.write("cd $TMPDIR\n")     #go to a temporary directory to execute your job

    #different jobs can still end up in the same temporary directory, so be sure to create a directory name UNIQUE to your job
    tmp_job.write("mkdir Job_%s\n"%str(i))  
    tmp_job.write("cd Job_%s\n"%str(i))

    #set cms environment
    tmp_job.write("cd %s\n"%(cmsEnv))    
    tmp_job.write("eval `scramv1 runtime -sh`\n")
    tmp_job.write("cd -\n")

    tmp_job.write("cp -f %s* .\n"%(jobDir)) #copy the content of your local job directory to the temporary directory
    tmp_job.write("cmsRun run_cfg.py\n")   #run the configuration file

    #Copy the output root file and remove it from the temporary directory
    #NOTE: "hlt.root" is hardcoded here
    tmp_job.write("echo 'sending the file back'\n")
    tmp_job.write("cp hlt.root %s/hlt_%s.root\n"%(remoteDir, str(i)))  #give a unique name (depending on the value of "i") when copying the output
    tmp_job.write("rm hlt.root\n")

    tmp_job.close()
    os.system("chmod +x %s"%(jobDir+tmp_jobname))  #make sure your condor file is executable by "anyone"

    print "preparing job number %s/%s"%(str(i), nJobs-1)  #information printed on the terminal about how many jobs were created and how many remain

    #only a subset of the files we loaded from "list_cff.py" will be used in THIS specific job
    iFileMin = i*opts.nPerJob
    iFileMax = (i+1)*opts.nPerJob

    #set these input files as source for "run_cfg.py", modifying "process.source"
    process.source.fileNames = fullSource.fileNames[iFileMin:iFileMax]
          
    tmp_cfgFile = open(jobDir+'/run_cfg.py','w')  #create the "run_cfg.py" file for this job
    tmp_cfgFile.write(process.dumpPython())   #dump all the info contained in "process" into the "run_cfg.py" file
    tmp_cfgFile.close()
    


#Write the content of the master condor file, allowing for the submission of all condor jobs
condor_str = "executable = $(filename)\n" #'$(filename)' is a variable defined below
if opts.proxyPath != "noproxy":
    #if you specified a proxy, include it in the condor arguments
    condor_str += "Proxy_path = %s\n"%opts.proxyPath
    condor_str += "arguments = $(Proxy_path) $Fp(filename) $(ClusterID) $(ProcId)\n"
else:
    condor_str += "arguments = $Fp(filename) $(ClusterID) $(ProcId)\n"  #'$(ClusterID)' and '$(ProcId)' are condor arguments which condor can generate automatically

#Set name of the TEXT outputs for the jobs (errors, stuff printed on terminal, log)
condor_str += "output = $Fp(filename)hlt.stdout\n"  
condor_str += "error = $Fp(filename)hlt.stderr\n"
condor_str += "log = $Fp(filename)hlt.log\n"

condor_str += '+JobFlavour = "%s"\n'%opts.jobFlavour #job flavour (sets maximum run time)

#condor syntax allows to specify all condor jobs we wish to run in one line
#in this case all file names matching the regular expression below
#'$(ClusterID)' and '$(ProcId)' arguments are auto generated for each job
condor_str += "queue filename matching ("+MYDIR+"/Jobs/Job_*/*.sh)"  
#END writing the content of the master file


condor_name = MYDIR+"/condor_cluster.sub" #name of the master condor file

#Create the master file and write the content
condor_file = open(condor_name, "w")
condor_file.write(condor_str)

#Write condor job submission command
sub_total.write("condor_submit %s\n"%condor_name)

#Modify "sub_total.jobb" to allow it to be executed
os.system("chmod +x sub_total.jobb")
