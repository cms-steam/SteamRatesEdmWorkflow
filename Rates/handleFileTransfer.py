import os


from aux import mergeNames
from optparse import OptionParser
parser=OptionParser()
parser.add_option("-d","--directory",dest="dir",type="str",default="nodir",help="working DIRECTORY",metavar="DIRECTORY")
parser.add_option("-m","--dataset",dest="MCdataset",type="str",default="Data",help="MCdataset string")
parser.add_option("-s","--finalstring",dest="fstr",type="str",default="nostr",help="final STRING",metavar="STRING")
parser.add_option("-S","--dirSuffix",dest="dirSuffix",type="str",default="",help="Suffix of the Results directory")

(opts, args) = parser.parse_args()

head_dir = opts.dir

dirSuffix = ""
if opts.dirSuffix
		dirSuffix = f"_{opts.dirSuffix}"

results_dir = "Results" + dirSuffix

os.system(f"mkdir {head_dir}/{results_dir}")
MCorData="MC"
MCorData2="MC/"+opts.MCdataset
inDir="Jobs/"+opts.MCdataset
if opts.MCdataset == "Data":
    MCorData = "Data"
    MCorData2 = "Data"
    inDir = "Jobs"
os.system(f"mkdir {head_dir}/{results_dir}/{MCorData}")
if opts.MCdataset != "Data":
    os.system(f"mkdir {head_dir}/{results_dir}/{MCorData2}")
rawDir=f"{head_dir}/{results_dir}/{MCorData2}/Raw"
os.system(f"mkdir {rawDir}")

for name in mergeNames:
    os.system(f"mkdir {rawDir}/{mergeNames[name]}")
os.system(f"mkdir {rawDir}/Root")
os.system(f"mkdir {rawDir}/Global")


for filename in list(mergeNames.keys()):
    os.system("cp %s/%s.%s.csv %s/%s/"%(inDir, filename, opts.fstr, rawDir, mergeNames[filename]))
    os.system("rm -f %s/%s.%s.csv"%(inDir, filename, opts.fstr))


os.system("cp %s/output.global.%s.csv %s/Global/"%(inDir, opts.fstr, rawDir))
os.system("rm -f %s/output.global.%s.csv"%(inDir, opts.fstr))
os.system("cp %s/histos.%s.root %s/Root/"%(inDir, opts.fstr, rawDir))
os.system("rm -f %s/histos.%s.root"%(inDir, opts.fstr))
