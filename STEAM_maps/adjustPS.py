import os, sys
from outputMaps import *

#DEBUG=False
DEBUG   = True
HLTPSED = False # if HLT PS were applied when predicting rates
DOUNPSL1= False # if want to L1-unprescale
THRESH  = 0.15  # threshold to consider that the HLT PS changes significantly

outputfile = open('adjusted.csv','w')
psnames='2.2e34,2.0e34,1.8e34,1.6e34,1.4e34,1.0e34,5.0e33'
outputfile.write('Path,Target rate,Target rate low, Predicted L1PSed rate,L1 Prescales,,,,,,,,,,HLT Prescales (old),,,,,,,,,,HLT Prescales (new),,,,,,,,,HLT Rates (old),,,,,,,,,HLT Rates (new),,,,,,,,Stream,Dataset\n')
outputfile.write(',,,,Emer'+psnames+',Emer'+(psnames*4)+',,SamePS?\n')

# Description of the PS columns
nCol=6

lumi =[22.,20.,18.,16.,14.,10.,5.0]
#lumi =[15.,14.,13.,12.,11.5,10.,7.5,5.0,2.5] # NEW LABELS AND TARGET LUMIS FOR HLT MENU v4 !!! 

iLumiL1T=1      # offset when reading L1 PS stored in maps: index of column "2.0e34"
iLumiHLT=2      # offset when reading HLT prescales (from confdb) stored in maps: index of column "2.0e34"
iOnlineL1=1     # index of L1 PS column corresponding to prescales used online 
iPrescaleHLT=2  # index of HLT PS applied when running the HLT
iLow=3          # index (in above list "lumi") of first PS column considered as "low" lumi (for targetLow)
iPredictLumi=2  # index (in above list "lumi") of PS column corresponding to lumi used to predict rates

nPaths=0

for path in streamsMap:
    
    #path2 = path.rstrip("0123456789")
    path2 = path

    # read maps
    stream     = streamsMap[path2][0]
    dataset    = datasetMap[path2][0]
    status     = statusMap[path2][0]
    unPS       = psL1TMap[path2][iOnlineL1]
    list_L1TPS = psL1TMap[path2]
    list_HLTPS = psHLTMap[path2][0:nCol+iLumiHLT]
    target     = rateMap[path2]
    #targetLow  = rateLowMap[path2]
    targetLow = -999

    # read rate predictions
    if path2 in predictMap:
        predict    = predictMap[path2]
        if predict==0:
            print "WARNING: path ",path2," has a null predicted rate"," (",status,")"
    else:
        print "ERROR: no rate predictions for path ",path2," (",status,")"
        continue

    # in case HLT PS were applied when running the HLT
    if HLTPSED:
        unPS *= list_HLTPS[ iPrescaleHLT ]

    if DEBUG:
        print "PATH: ",path2,status,'(',target,targetLow,')',predict,unPS

    # check only prescaled paths
    if not status=="prescaled":
        #if DEBUG:
        #    print "PATH: ",path2,status
        continue
    nPaths += 1

    # unprescaled predicted rate
    if DOUNPSL1:
        predictUnPs = predict*unPS 
    else:
        predictUnPs = predict # in this case, predictions.csv is already unprescaled

    # total PS needed to meet target rate...
    totPS      = [-1] * nCol
    hltPS      = [-1] * nCol
    oldRate    = [-1]* nCol
    oldRateDev = [-1]* nCol
    newRate    = [-1]* nCol
    newRateDev = [-1]* nCol

    samePS=True # check if at least one of the HLT PS value changes

    for j in range(0, nCol):
        #
        # high lumi columns: default
        theTarget = target 
        if DEBUG: 
            print '--- iter #'+str(j)
        # low lumi columns if specific request
        if j>=iLow and targetLow>0: 
            theTarget = targetLow
        if DEBUG: 
            print '--- theTarget =',theTarget
        #
        lumiSF  = lumi[j] / lumi[iPredictLumi]
        theL1PS = list_L1TPS[ iLumiL1T+j ]
        if DEBUG: 
            print "lumiSF  = lumi[j] / lumi[iPredictLumi] = ", lumi[j], "/", lumi[iPredictLumi], "=", lumiSF
            print "theL1PS = list_L1TPS[ iLumiL1T+j ] =", theL1PS
        #
        if theTarget>0:
            totPS[j] = predictUnPs * lumiSF / theTarget
            if DEBUG:
                print "totPS[j] = predictUnPs * lumiSF / theTarget = ", predictUnPs, "*", lumiSF, "/", theTarget
        #
        if theL1PS!=0:
            hltPS[j] = float(totPS[j]) / float(theL1PS)
            if DEBUG:
                print "hltPS[j] = float(totPS[j]) / float(theL1PS) = ", float(totPS[j]), "/", float(theL1PS)
            if hltPS[j]>0 and hltPS[j]<1:
                hltPS[j]=1
                if DEBUG:
                    print "hltPS[j]>0 and hltPS[j]<1 => hltPS[j]=", hltPS[j]
            else:
                hltPS[j] = round( hltPS[j] , 0 )
                if DEBUG:
                    print "hltPS[j] = round( hltPS[j] , 0 ) = ", hltPS[j]
        #
        if DEBUG:
            print "DEBUG: j=",j
        if theL1PS!=0 and list_HLTPS[ iLumiHLT+j ]!=0:
            oldRate[j]    = round( (predictUnPs * lumiSF) / ( theL1PS * list_HLTPS[ iLumiHLT+j ] ) , 2 )
        if theL1PS!=0 and hltPS[j]!=0:
            newRate[j]    = round( (predictUnPs * lumiSF) / ( theL1PS * hltPS[j] ) , 2 )
        #
        a=float(hltPS[j])
        b=float(list_HLTPS[ iLumiHLT+j ])
        if (b+a)>0 and abs(2*(b-a)/(b+a))>THRESH:
            samePS=False
        
    wordL1PS     = ','.join('%s' % p for p in list_L1TPS)
    wordHLTPSOld = ','.join('%s' % p for p in list_HLTPS)
    wordHLTPSNew = ','.join('%s' % p for p in hltPS)
    wordOldRate  = ','.join('%s' % p for p in oldRate)
    wordNewRate  = ','.join('%s' % p for p in newRate)

    myWords    = [path2,target,targetLow,predict,wordL1PS,wordHLTPSOld,wordHLTPSNew,wordOldRate,wordNewRate,stream,dataset,samePS]
    mySentence = ','.join('%s' % w for w in myWords)
    outputfile.write(mySentence+'\n')

    #print '\n'+mySentence+'\n'
    print '\n'+path2+" Targets=("+str(target)+','+str(targetLow)+') Predict='+str(predict)+' L1=['+wordL1PS+'] HLT_old=['+wordHLTPSOld+'] HLT_new=['+wordHLTPSNew+'] Rate_old=['+wordOldRate+'] Rate_new=['+wordNewRate+']\n'

print "Treated",nPaths,"prescaled paths"
