import os


from aux import mergeNames
from optparse import OptionParser
parser=OptionParser()
parser.add_option("-d","--directory",dest="dir",type="str",default="nodir",help="working DIRECTORY",metavar="DIRECTORY")

(opts, args) = parser.parse_args()

try:
    os.system("mkdir %s/Results"%opts.dir)
    os.system("mkdir %s/Results/Raw"%opts.dir)
    for name in mergeNames:
        os.system("mkdir %s/Results/Raw/%s"%(opts.dir, mergeNames[name]))
    os.system("mkdir %s/Results/Raw/Root"%opts.dir)
    os.system("mkdir %s/Results/Raw/Global"%opts.dir)
except:
    print "err!"
    pass


for filename in mergeNames.keys():
    try:
        os.system("cp %s*.csv %s/Results/Raw/%s"%(filename, opts.dir, mergeNames[filename]))
        os.system("rm -f %s*.csv"%filename)
    except:
        pass

os.system("cp output.global*.csv %s/Results/Raw/Global"%opts.dir)
os.system("rm -f output.global*.csv")
os.system("cp histo*.root %s/Results/Raw/Root"%opts.dir)
os.system("rm -f histo*.root")
