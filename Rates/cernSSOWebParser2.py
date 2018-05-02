
# -----------------------------------------------------------------------------
# Name:        cern sso website parser
# Purpose:     simple function to read the content of a cern sso protected website in python
#              highly wbm biased
#
# Author:      Sam Harper
#
# Created:     01.08.2015
# Copyright:   (c) Sam Harper 2015
# Licence:     GPLv3
# -----------------------------------------------------------------------------

from htmlTableParser import HTMLTableParser

import ssl
from functools import wraps
def sslwrap(func):
    @wraps(func)
    def bar(*args, **kw):
        kw['ssl_version'] = ssl.PROTOCOL_TLSv1
        return func(*args, **kw)
    return bar

def printCookieHelp():
    cmdToGetWBMSSOCookie="cern-get-sso-cookie --krb -r -u https://cmswbm.web.cern.ch/cmswbm -o $SSO_COOKIE"
    print "a SSO cookie be obtained by typing \"%s\"" % cmdToGetWBMSSOCookie
    print "more information can be found at http://linux.web.cern.ch/linux/docs/cernssocookie.shtml"
      

def readURL(url):
    import cookielib, urllib2,os,sys
    from subprocess import call

    cookieLocation=os.environ.get('SSO_COOKIE')
    #print cookieLocation
    if cookieLocation==None:
        print "please set the enviroment varible SSO_COOKIE to point to the location of the CERN SSO cookie"
        printCookieHelp()
        sys.exit()

    import os.path
    if os.path.isfile(cookieLocation)==False:
        print "cookie %s does not exist " % cookieLocation
        printCookieHelp()
        sys.exit()

    pythonVersionStr=sys.version.split()[0].split(".")
    pythonVersion=float(pythonVersionStr[0]+"."+pythonVersionStr[1])
    if pythonVersion<2.7:
        print "Warning python version is: "
        print sys.version
        print "problems have been encountered in 2.6, suggest you move to>= 2.7 (CMSSW version)"
        import time
        time.sleep(5)

    thepath = os.path.join('cmswbm.cern.ch','cmsdb','servlet')
    try:
        files = os.listdir(thepath)
        for file_tmp in files:
            os.remove(os.path.join(thepath,file_tmp))
    except:
        pass
    call(["wget", "--load-cookies", cookieLocation,"-q", "-p",url ])
    print url
    files = os.listdir(thepath)
    file_in = ''
    for file_tmp in files:
        file_in = os.path.join(thepath,file_tmp)
        break
    #print file_in
    
    return open(file_in,'r').read()
    

def parseURLTables(url):
    parser = HTMLTableParser()
    parser.feed(readURL(url))
    try:
        if parser.titles[0]=="Cern Authentication":
            print "cern authentication page detected, you need to renew your sso cookie"
            printCookieHelp()
            import time
            time.sleep(5)
    except:
        pass
    return parser.tables

#ssl.wrap_socket = sslwrap(ssl.wrap_socket)
#url = "https://cmswbm.cern.ch/cmsdb/servlet/HLTSummary?fromLS=1&toLS=3&RUN=262269&KEY=2000920"
#url = "https://cmswbm.cern.ch/cmsdb/servlet/RunSummary?RUN=284035&DB=default"
#print parseURLTables(url)
