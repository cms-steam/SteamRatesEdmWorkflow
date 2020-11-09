#!/usr/bin/env python

hardCodedDataset = "/RelValZEE_13/CMSSW_10_4_0-103X_upgrade2018_realistic_v8-v1/GEN-SIM-DIGI-RAW"

import os, sys,  imp, re, pprint, string
from optparse import OptionParser

# cms specific
import FWCore.ParameterSet.Config as cms

import time
import datetime
import os
import sys
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


help_text = '\n./cmsCondor.py <cfgFileName> <CMSSWrel> <remoteDir> -p <proxyPath> -n <nPerJob> -q <jobFlavour>'
help_text += '\n<cfgFileName> (mandatory) = name of your configuration file (e.g. hlt_config.py)'
help_text += '\n<CMSSWrel> (mandatory) = directory where the top of a CMSSW release is located'
help_text += '\n<remoteDir> (mandatory) = directory where the files will be transfered (e.g. on EOS)'
help_text += '\n<proxyPath> (optional) = location of your voms cms proxy. Note: keep your proxy in a private directory.'
help_text += '\n<nPerJob> (optional) = number of files processed per batch job (default=5)'
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

fileDatasetMap={}

#retrieve MC datasets, query DAS, and make a list of input files
if opts.proxyPath != "noproxy":
    fileList=open('list_cff.py','w')
    fileList.write("inputFileNames=[\n")
    from map_MCdatasets_xs import datasetCrossSectionMap
    for dataset in datasetCrossSectionMap.keys():
        das_command = runCommand('dasgoclient --query="file dataset=%s"'%dataset)
        stdout, stderr = das_command.communicate()
    
        for line in stdout.splitlines():
            fileList.write("'"+line+"',\n")
            fileDatasetMap[line]=dataset
    
    fileList.write("]\n")
    fileList.close()
else:
    from list_cff import inputFileNames
    for ffile in inputFileNames:
        fileDatasetMap[ffile]=hardCodedDataset
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
    print "(approximate) number of jobs to be created: ", nJobs
        
    
datasetList=[]
if opts.proxyPath == "noproxy":
    datasetList.append(hardCodedDataset)
else:
    datasetList=datasetCrossSectionMap.keys()

jobCount=0
last_kFileMax=0
for dataset in datasetList:
    datasetName=dataset.lstrip("/")
    datasetName=datasetName.replace("/","_")
    datasetJobDir='Jobs/'+datasetName
    datasetRemoteDir=remoteDir+'/'+datasetName
    os.system('mkdir '+datasetJobDir)
    os.system('mkdir '+datasetRemoteDir)

    print "dataset: ", dataset
    #make job scripts
    keepGoing=True
    i=0
    while (keepGoing):
        #print 'total: %d/%d  ;  %.1f %% processed '%(j,my_sum,(100*float(j)/float(my_sum)))
    
        jobDir = MYDIR+"/"+datasetJobDir+'/Job_%s/'%str(i)
        os.system('mkdir %s'%jobDir)
    
        tmp_jobname="sub_%s.sh"%(str(i))
        tmp_job=open(jobDir+tmp_jobname,'w')
        tmp_job.write("#!/bin/sh\n")
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

        kFileMin = last_kFileMax+i*opts.nPerJob
        kFileMax = last_kFileMax+(i+1)*opts.nPerJob

        i+=1


        if (kFileMax < len(fullSource.fileNames)):
            while (fileDatasetMap[fullSource.fileNames[kFileMax-1]] != dataset):
                kFileMax -= 1
                keepGoing=False
            if fileDatasetMap[fullSource.fileNames[kFileMax]] != dataset: keepGoing=False
        else:
            keepGoing=False
        if not keepGoing: last_kFileMax = kFileMax
              
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
