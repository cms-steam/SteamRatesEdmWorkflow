#!/usr/bin/env python

#Create condor jobs for running over MC files
#Check cmsCondorData.py for more comments, the structure of the script is very similar
#The main difference: in MC we run over many datasets at once, and the recommended option is to generate automatically "list_cff.py"

#The hard coded dataset is only used when running with no proxy, you may wish to drop it
hardCodedDataset = "/RelValZEE_13/CMSSW_10_4_0-103X_upgrade2018_realistic_v8-v1/GEN-SIM-DIGI-RAW"

import os, sys,  imp, re, pprint, string

# cms specific
import FWCore.ParameterSet.Config as cms

import time
import datetime
import shlex
import subprocess

def runCommand(commandLine):
    #sys.stdout.write("%s\n" % commandLine)
    args = shlex.split(commandLine)
    retVal = subprocess.Popen(args, stdout = subprocess.PIPE)
    return retVal



MYDIR=os.getcwd()
#folder = '/store/group/dpg_trigger/comm_trigger/TriggerStudiesGroup/STEAM/Summer16_FlatPU28to62/HLTRates_v4p2_V2_1p25e34_MC_2017feb09J'

#get inputs
from optparse import OptionParser
parser=OptionParser()
parser.add_option("-n",dest="nPerJob",type="int",default=1,help="NUMBER of files processed per job",metavar="NUMBER")
parser.add_option("-q","--flavour",dest="jobFlavour",type="str",default="workday",help="job FLAVOUR",metavar="FLAVOUR")
parser.add_option("-p","--proxy",dest="proxyPath",type="str",default="noproxy",help="Proxy path")

opts, args = parser.parse_args()


help_text = '\n./cmsCondorMC.py <cfgFileName> <CMSSWrel> <remoteDir> -p <proxyPath> -n <nPerJob> -q <jobFlavour>'
help_text += '\n<cfgFileName> (mandatory) = name of your configuration file (e.g. hlt_config.py)'
help_text += '\n<CMSSWrel> (mandatory) = directory where the top of a CMSSW release is located'
help_text += '\n<remoteDir> (mandatory) = directory where the files will be transfered (e.g. on EOS)'
help_text += '\n<proxyPath> (optional) = location of your voms cms proxy. Note: keep your proxy in a private directory.'
help_text += '\n<nPerJob> (optional) = number of files processed per condor job (default=1)'
help_text += '\n<flavour> (optional) = job flavour (default=workday)\n'


cfgFileName = str(args[0])
cmsEnv = str(args[1])
remoteDir = str(args[2])

print 'config file = %s'%cfgFileName
print 'CMSSWrel = %s'%cmsEnv
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


sub_total = open("sub_total.jobb","w")

#copy MC datasets file here so it can be used
os.system("cp ../MCDatasets/map_MCdatasets_xs.py .")

fileDatasetMap={}  #create map from the input files to the datasets

if opts.proxyPath != "noproxy": #if you're using a proxy (recommended)
    #retrieve MC datasets, query DAS, and make a list of input files
    #The list of input files is created automatically, this will override any existing "list_cff.py"
    fileList=open('list_cff.py','w')
    fileList.write("inputFileNames=[\n")
    from map_MCdatasets_xs import datasetCrossSectionMap
    for dataset in datasetCrossSectionMap.keys():
        #find the files corresponding to each dataset
        das_command = runCommand('dasgoclient --query="file dataset=%s"'%dataset) 
        stdout, stderr = das_command.communicate()
    
        for line in stdout.splitlines(): #Each line corresponds to one file
            fileList.write("'"+line+"',\n")  #update list of files
            fileDatasetMap[line]=dataset  #update file->dataset map
    
    fileList.write("]\n")
    fileList.close()
else:  #if you're NOT using a proxy (not recommended)
    from list_cff import inputFileNames   #you need to enter the file names manually into "list_cff.py"
    for ffile in inputFileNames:
        fileDatasetMap[ffile]=hardCodedDataset   #all files should belong to your hardcoded dataset. The code doesn't check this, you need to check it yourself.
    print fileDatasetMap

# load cfg script
handle = open(cfgFileName, 'r')
cfo = imp.load_source("pycfg", cfgFileName, handle)
process = cfo.process
handle.close()
    
# keep track of the original source
fullSource = process.source.clone()
    
    
nJobs = -1
    
try:
    process.source.fileNames
except:
    print 'No input file. Exiting.'
    sys.exit(2)
else:
    print "Number of files in the source:",len(process.source.fileNames), ":"
    pprint.pprint(process.source.fileNames)
       
    nFiles = len(process.source.fileNames)
    nJobs = nFiles / opts.nPerJob
    if (nJobs!=0 and (nFiles % opts.nPerJob) > 0) or nJobs==0:
        nJobs = nJobs + 1
        
    #print "dataset: ", dataset
    print "(approximate) number of jobs to be created: ", nJobs #the number of jobs could be off by one if the number of files per job doesn't exactly divide the total number of files
        

#Create a list of datasets    
datasetList=[]
if opts.proxyPath == "noproxy":
    datasetList.append(hardCodedDataset)
else:
    datasetList=datasetCrossSectionMap.keys()


#Run over the list of datasets
jobCount=0
last_kFileMax=0 
for dataset in datasetList:
    #Replace the "/" in the dataset names by "_" so we can create a proper directory name from the dataset name
    datasetName=dataset.lstrip("/")
    datasetName=datasetName.replace("/","_")
    datasetJobDir='Jobs/'+datasetName
    datasetRemoteDir=remoteDir+'/'+datasetName
    os.system('mkdir '+datasetJobDir)
    os.system('mkdir '+datasetRemoteDir)

    print "dataset: ", dataset
    #loop creating jobs
    keepGoing=True
    i=0
    while (keepGoing): #start JOB WHILE loop
        #print 'total: %d/%d  ;  %.1f %% processed '%(j,my_sum,(100*float(j)/float(my_sum)))
        
        #Create dedicated directory for each job
        jobDir = MYDIR+"/"+datasetJobDir+'/Job_%s/'%str(i)
        os.system('mkdir %s'%jobDir)
    
        tmp_jobname="sub_%s.sh"%(str(i))  #name for the condor job file
        tmp_job=open(jobDir+tmp_jobname,'w')  #open the condor file
        tmp_job.write("#!/bin/sh\n")  #explain to condor it's *.sh script
        if opts.proxyPath != "noproxy":
            tmp_job.write("export X509_USER_PROXY=$1\n")
            tmp_job.write("voms-proxy-info -all\n")
            tmp_job.write("voms-proxy-info -all -file $1\n")
        tmp_job.write("ulimit -v 5000000\n")
        tmp_job.write("cd $TMPDIR\n")
        tmp_job.write("mkdir Job_%s\n"%str(i))
        tmp_job.write("cd Job_%s\n"%str(i))
        tmp_job.write("cd %s\n"%(cmsEnv))
        tmp_job.write("eval `scramv1 runtime -sh`\n")
        tmp_job.write("cd -\n")
        tmp_job.write("cp -f %s* .\n"%(jobDir))
        tmp_job.write("cmsRun run_cfg.py\n")
        tmp_job.write("echo 'sending the file back'\n")
        tmp_job.write("cp hlt.root %s/hlt_%s.root\n"%(datasetRemoteDir, str(i)))
        tmp_job.write("rm hlt.root\n")
        tmp_job.close()
        os.system("chmod +x %s"%(jobDir+tmp_jobname))
    
        print "preparing job number %s"%str(jobCount)
        jobCount += 1

        #in the list of input files, select only a few (number determined by option)
        kFileMin = last_kFileMax+i*opts.nPerJob
        kFileMax = last_kFileMax+(i+1)*opts.nPerJob

        i+=1


        if (kFileMax < len(fullSource.fileNames)): #check that we're not going over the total number of input files
            while (fileDatasetMap[fullSource.fileNames[kFileMax-1]] != dataset):
                #if the last file we want to use for this job DOES NOT belong to the correct dataset
                #last file is "fileNames[kFileMax-1]" because "kFileMax" is an EXCLUSIVE upper limit
                kFileMax -= 1    #lower the upper limit by 1 (until we find a file belonging to the CORRECT dataset)
                keepGoing=False  #tells us we're done with this dataset, ready to move to the next
            if fileDatasetMap[fullSource.fileNames[kFileMax]] != dataset: keepGoing=False  #tells us we're done with this dataset, ready to move to the next
        else:  #we ARE over the total number of input files
            keepGoing=False
        if not keepGoing: last_kFileMax = kFileMax  #variable telling where to start looking for files for the NEXT dataset
              
        #input files
        process.source.fileNames = fullSource.fileNames[kFileMin:kFileMax]


        tmp_cfgFile = open(jobDir+'/run_cfg.py','w')
        tmp_cfgFile.write(process.dumpPython())
        tmp_cfgFile.close()
    


condor_str = "executable = $(filename)\n"
if opts.proxyPath != "noproxy":
    condor_str += "Proxy_path = %s\n"%opts.proxyPath
    condor_str += "arguments = $(Proxy_path) $Fp(filename) $(ClusterID) $(ProcId)\n"
else:
    condor_str += "arguments = $Fp(filename) $(ClusterID) $(ProcId)\n"
condor_str += "output = $Fp(filename)hlt.stdout\n"
condor_str += "error = $Fp(filename)hlt.stderr\n"
condor_str += "log = $Fp(filename)hlt.log\n"
condor_str += '+JobFlavour = "%s"\n'%opts.jobFlavour
condor_str += "queue filename matching ("+MYDIR+"/Jobs/*/Job_*/*.sh)"
condor_name = MYDIR+"/condor_cluster.sub"
condor_file = open(condor_name, "w")
condor_file.write(condor_str)
sub_total.write("condor_submit %s\n"%condor_name)
os.system("chmod +x sub_total.jobb")
