import os


from aux import mergeNames
from optparse import OptionParser
parser=OptionParser()
parser.add_option("-d","--directory",dest="dir",type="str",default="nodir",help="working DIRECTORY",metavar="DIRECTORY")
parser.add_option("-s","--finalstring",dest="fstr",type="str",default="nostr",help="final STRING",metavar="STRING")

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
    os.system("cp Jobs/%s.%s.csv %s/Results/Raw/%s"%(filename, opts.fstr, opts.dir, mergeNames[filename]))
    os.system("rm -f Jobs/%s.%s.csv"%(filename, opts.fstr))


os.system("cp Jobs/output.global.%s.csv %s/Results/Raw/Global"%(opts.fstr, opts.dir))
os.system("rm -f Jobs/output.global.%s.csv"%opts.fstr)
os.system("cp Jobs/histos.%s.root %s/Results/Raw/Root"%(opts.fstr, opts.dir))
os.system("rm -f Jobs/histos.%s.root"%opts.fstr)
