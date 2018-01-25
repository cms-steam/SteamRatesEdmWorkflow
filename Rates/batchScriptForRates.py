import time
import datetime
import os
import sys
from filesInput import fileInputNames
from aux import runCommand

MYDIR=os.getcwd()
#folder = '/store/group/dpg_trigger/comm_trigger/TriggerStudiesGroup/STEAM/Summer16_FlatPU28to62/HLTRates_v4p2_V2_1p25e34_MC_2017feb09J'


#make directories for the jobs
try:
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
i=0
loop_mark = 1
#make job scripts
tmp_text=''

k = 0
pre_k = 0
for infile in fileInputNames:
    #print 'total: %d/%d  ;  %.1f %% processed '%(j,my_sum,(100*float(j)/float(my_sum)))

    tmp_jobname="sub_%s.jobb"%(str(i))
    tmp_job=open(MYDIR+'/Jobs/sub_job/'+tmp_jobname,'w')
    tmp_job.write("curr_dir=%s\n"%(MYDIR))
    tmp_job.write("cd %s\n"%(MYDIR))
    tmp_job.write("source env.sh\n")
    tmp_job.write("python triggerRatesFromTriggerResults.py -i %s\n"%(infile))
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
        sub_str = "bsub -q 8nh -eo Jobs/sub_err/err_%s.dat -oo Jobs/sub_out/out_%s.dat %s"%(k,k,Tjob_dir)
        local_str = "%s"%(Tjob_dir)
        #os.system(sub_str)
        sub_total.write("%s\n"%(sub_str))
        pre_k = k
        tmp_text = ''
os.system("chmod +x sub_total.jobb")


