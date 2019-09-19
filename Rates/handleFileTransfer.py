import os


from aux import mergeNames
from optparse import OptionParser
parser=OptionParser()
parser.add_option("-d","--directory",dest="dir",type="str",default="nodir",help="working DIRECTORY",metavar="DIRECTORY")
parser.add_option("-m","--dataset",dest="MCdataset",type="str",default="Data",help="MCdataset string")
parser.add_option("-s","--finalstring",dest="fstr",type="str",default="nostr",help="final STRING",metavar="STRING")

(opts, args) = parser.parse_args()

os.system("mkdir %s/Results"%opts.dir)
MCorData="MC"
MCorData2="MC/"+opts.MCdataset
inDir="Jobs/"+opts.MCdataset
if opts.MCdataset == "Data":
    MCorData = "Data"
    MCorData2 = "Data"
    inDir = "Jobs"
os.system("mkdir %s/Results/%s"%(opts.dir, MCorData))
if opts.MCdataset != "Data":
    os.system("mkdir %s/Results/%s"%(opts.dir, MCorData2))
rawDir="%s/Results/%s/Raw"%(opts.dir, MCorData2)
os.system("mkdir %s"%rawDir)
    
for name in mergeNames:
    os.system("mkdir %s/%s"%(rawDir, mergeNames[name]))
os.system("mkdir %s/Root"%rawDir)
os.system("mkdir %s/Global"%rawDir)


for filename in mergeNames.keys():
    os.system("cp %s/%s.%s.csv %s/%s/"%(inDir, filename, opts.fstr, rawDir, mergeNames[filename]))
    os.system("rm -f %s/%s.%s.csv"%(inDir, filename, opts.fstr))


os.system("cp %s/output.global.%s.csv %s/Global/"%(inDir, opts.fstr, rawDir))
os.system("rm -f %s/output.global.%s.csv"%(inDir, opts.fstr))
os.system("cp %s/histos.%s.root %s/Root/"%(inDir, opts.fstr, rawDir))
os.system("rm -f %s/histos.%s.root"%(inDir, opts.fstr))
