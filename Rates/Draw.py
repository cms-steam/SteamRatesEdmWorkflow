'''
                   Draw dataset/dataset and trigger/dataset overlap rates 
                   Draw pie charts for group rates
'''
import ROOT
import math
from Menu_HLT import datasetMap as  triggersDatasetMap
from aux import physicsStreamOK
from aux import datasetOK
from aux import datasets_for_corr as good_datasets
from aux import reorderList
from aux import mapForDecreasingOrder
import matplotlib as mpl
import matplotlib.pyplot as plt
import numpy as np
import csv
import os


#auxiliary function
def newDatasetOK(dataset, newDMap):
    keepGoing = False
    if datasetOK(dataset):
        keepGoing = True
    else:
        for old_dataset in list(newDMap.keys()):
            if not (dataset in newDMap[old_dataset]): continue
            if datasetOK(old_dataset):
                keepGoing = True
                break
    return keepGoing


from optparse import OptionParser
parser=OptionParser()
parser.add_option("-m","--merging",dest="newDataset",type="str",default="no",help="Are you testing the merging of datasets? yes/no, default = no")
parser.add_option("-d","--directory",dest="inputDir",type="str",default="Results/Data",help="input DIRECTORY", metavar="DIRECTORY")

opts, args = parser.parse_args()

if opts.newDataset == "yes":
    from Menu_HLT import newDatasetMap


os.system("mkdir Figures")
outputDir=""
if opts.inputDir == "Results/Data":
    outputDir="Figures/Data"
    os.system("mkdir %s"%outputDir)
else:
    os.system("mkdir Figures/MC")
    subdir=opts.inputDir
    lastSlash = subdir.rfind('/')
    subdir = subdir[lastSlash:]
    outputDir="Figures/MC/%s"%subdir
    os.system("mkdir %s"%outputDir)

#Pie charts
naive_group_label_total1=[]
naive_group_label_total2=[]
naive_group_total=[]
naive_group_label_pure=[]
naive_group_pure=[]
naive_group_label_pureplusshared=[]
naive_group_pureplusshared=[]
colors_total=[]
colors_pure=[]
colors_pureplusshared=[]
basic_colors = ['gold', 'goldenrod', 'yellow', 'yellowgreen', 'green', 'lightgreen', 'lime', 'turquoise', 'teal', 'lightblue', 
#'navy', 
'orchid', 'pink', 'tomato', 'red', 'crimson', 'orangered', 'orange', 'sienna', 'brown']
with open("%s/output.group.csv"%opts.inputDir) as group_file:
    reader=csv.reader(group_file, delimiter=',')
    skip = True #skip 1st line
    total_rate = 0
    pure_rate = 0
    pureplusshared_rate = 0
    for row in reader:
        if skip:
            skip = False
            continue
        total_rate += float(row[2])
        pure_rate += float(row[4])
        pureplusshared_rate += float(row[6])

    skip = True #skip 1st line

n_total=0
n_pure=0
n_pureplusshared=0
others_total_rate = 0
others_pure_rate = 0
others_pureplusshared_rate = 0


with open("%s/output.group.csv"%opts.inputDir) as group_file:

    reader=csv.reader(group_file, delimiter=',')
    for row in reader:
        if skip:
            skip = False
            continue
        if (total_rate > 0 and float(row[2])/total_rate > 0.02):
            naive_group_label_total1.append(row[0])
            naive_group_label_total2.append(str(int(round(float(row[2]),0))))
            naive_group_total.append(float(row[2]))
            colors_total.append(basic_colors[n_total % len(basic_colors)])
            n_total += 1
        else:
            others_total_rate += float(row[2])
        if (pure_rate > 0 and float(row[4])/pure_rate > 0.02):
            naive_group_label_pure.append(row[0] + "\n" + str(int(round(float(row[4]),0))) + " Hz")
            naive_group_pure.append(float(row[4]))
            colors_pure.append(basic_colors[n_pure % len(basic_colors)])
            n_pure += 1
        else:
            others_pure_rate += float(row[4])
        if (pureplusshared_rate > 0 and float(row[6])/pureplusshared_rate > 0.02):
            naive_group_pureplusshared.append(float(row[6]))
            naive_group_label_pureplusshared.append(row[0] + "\n" + str(int(round(float(row[6]),0))) + " Hz")
            colors_pureplusshared.append(basic_colors[n_pureplusshared % len(basic_colors)])
            n_pureplusshared += 1
        else:
            others_pureplusshared_rate += float(row[6])

            
map_total = mapForDecreasingOrder(naive_group_total)
print(map_total)
group_total = reorderList(naive_group_total, map_total)
group_label_total1 = reorderList(naive_group_label_total1, map_total)
group_label_total2 = reorderList(naive_group_label_total2, map_total)
if others_total_rate > 0:
    group_label_total1.append('Others')
    group_label_total2.append(str(int(round(others_total_rate,0))))
    n_total +=1
    group_total.append(others_total_rate)
    colors_total.append(basic_colors[n_total % len(basic_colors)])

map_pure = mapForDecreasingOrder(naive_group_pure)
group_pure = reorderList(naive_group_pure, map_pure)
group_label_pure = reorderList(naive_group_label_pure, map_pure)
if others_pure_rate > 0:
    group_label_pure.append('Others\n' + str(int(round(others_pure_rate,0))) + " Hz")
    n_pure +=1
    group_pure.append(others_pure_rate)
    colors_pure.append(basic_colors[n_pure % len(basic_colors)])

map_pureplusshared = mapForDecreasingOrder(naive_group_pureplusshared)
group_pureplusshared = reorderList(naive_group_pureplusshared, map_pureplusshared)
group_label_pureplusshared = reorderList(naive_group_label_pureplusshared, map_pureplusshared)
if others_pureplusshared_rate > 0:
    group_label_pureplusshared.append('Others\n' + str(int(round(others_pureplusshared_rate,0))) + " Hz")
    n_pureplusshared +=1
    group_pureplusshared.append(others_pureplusshared_rate)
    colors_pureplusshared.append(basic_colors[n_pureplusshared % len(basic_colors)])


mpl.rcParams['xtick.labelsize'] = 10
mpl.rcParams['ytick.labelsize'] = 10

width = 0.5
ind = np.arange(len(group_total))
fig, ax = plt.subplots()
barchart = ax.bar(ind, group_total, width, color=colors_total)
ax.set_ylabel('Rates (Hz)')
ax.set_xticks(ind + width/2.)
ax.set_xticklabels(group_label_total1)
j = 0
for rect in barchart:
    height = rect.get_height()
    ax.text(rect.get_x() + rect.get_width()/2., height+0.02, group_label_total2[j], ha='center', va='bottom', fontsize = 10)
    j += 1
plt.title('Group Total Rates', fontweight='bold', y = 1.02)
plt.savefig("%s/group_total.pdf"%outputDir)

plt.clf()
#autopct='%1.1f%%', startangle=140, colors=colors)
plt.pie(group_pure, labels=group_label_pure, startangle=0, colors=colors_pure, autopct='%1.0f%%')
plt.title('Pure Group Rates', fontweight='bold', y = 1.08)
plt.axis('equal')
plt.savefig("%s/group_pure.pdf"%outputDir)


plt.clf()
plt.pie(group_pureplusshared, labels=group_label_pureplusshared, startangle=0, colors=colors_pureplusshared, autopct='%1.0f%%')
plt.title('Shared Group Rates', fontweight='bold', y = 1.08)
plt.axis('equal')
plt.savefig("%s/group_pureplusshared.pdf"%outputDir)
    


#Overlap plots
label_size=0.022
max_y_entries = 40
max_y_label_length = 67

c_dD=ROOT.TCanvas("canvas_dD","",0,0,900,900)
c_dD.SetGrid()
c_dD.SetLeftMargin(0.19)
c_dD.SetRightMargin(0.12)
c_dD.SetBottomMargin(0.19)


#some ROOT cosmetics
kInvertedDarkBodyRadiator=56
kAzure = 860
color = kAzure+7
ROOT.gStyle.SetFrameLineWidth(3)
ROOT.gStyle.SetLineWidth(3)
ROOT.gStyle.SetOptStat(0)
ROOT.gStyle.SetPalette(kInvertedDarkBodyRadiator)


datasets = {}
for trigger in list(triggersDatasetMap.keys()):
    strippedTrigger = trigger.rstrip("0123456789")
    datasets.update({str(strippedTrigger):triggersDatasetMap[trigger]})


#file=ROOT.TFile("final.root","r")
root_file=ROOT.TFile("%s/histos.root"%opts.inputDir,"R")
#root_file=ROOT.TFile("%s/Raw/Root/corr_histos_120.root"%opts.inputDir,"R")

tD_histo=root_file.Get("trigger_dataset_corr")
dD_histo=root_file.Get("dataset_dataset_corr")

triggerDataset_histo=[]
icount=-1
for i in range(0,tD_histo.GetNbinsX()):
    dataset = tD_histo.GetXaxis().GetBinLabel(i+1)
    if not datasetOK(dataset): continue
    nbinsY=0
    triggerList=[]
    for j in range(1,tD_histo.GetNbinsY()+1):
        trigger = tD_histo.GetYaxis().GetBinLabel(j)
        if not trigger in datasets: continue
        if dataset in datasets[trigger] and physicsStreamOK(trigger):
            nbinsY += 1
            triggerList.append(trigger)
    if (nbinsY <= 0): continue
    n = nbinsY//max_y_entries + 1
    equi_div = nbinsY//n + 1
    if (nbinsY > max_y_entries):
        for nn in range(0,n):
            nnbinsY = 1
            if (nn == n-1):
                nnbinsY = nbinsY-equi_div*nn
            else:
                nnbinsY = equi_div
            triggerDataset_histo.append(ROOT.TH2F("tD_"+dataset+str(nn),"Trigger-Dataset Overlap Rates (Hz)",len(good_datasets)+1,0,len(good_datasets)+1,nnbinsY,0,nnbinsY))
            icount+=1
    else:
        triggerDataset_histo.append(ROOT.TH2F("tD_"+dataset,"Trigger-Dataset Overlap Rates (Hz)",len(good_datasets)+1,0,len(good_datasets)+1,nbinsY,0,nbinsY))
        icount+=1

    #Fill plots with only the "interesting" datasets and paths
    jjj=0
    for jj in range(1,tD_histo.GetNbinsY()+1):
            trigger = tD_histo.GetYaxis().GetBinLabel(jj)
            if not trigger in triggerList: continue
            jjj+=1

            n = nbinsY//max_y_entries
            nn = (jjj-1)//equi_div
            inumber=icount - n + nn
            jnumber=jjj-nn*equi_div

            iii=0
            for ii in range(1,tD_histo.GetNbinsX()):
                dataset2 = tD_histo.GetXaxis().GetBinLabel(ii)
                if not datasetOK(dataset2): continue
                iii+=1
                triggerDataset_histo[inumber].GetXaxis().SetBinLabel(iii, dataset2)

                y_bin_label = trigger
                if len(trigger) > max_y_label_length: y_bin_label = trigger[:max_y_label_length]
                triggerDataset_histo[inumber].GetYaxis().SetBinLabel(jnumber, y_bin_label)
                bin_content = round(tD_histo.GetBinContent(ii, jj),2)
                if bin_content > 5: bin_content = round(tD_histo.GetBinContent(ii, jj),0)
                triggerDataset_histo[inumber].SetBinContent(iii, jnumber, bin_content)

                if dataset2 in datasets[trigger]:
                    iii+=1
                    triggerDataset_histo[inumber].GetXaxis().SetBinLabel(iii, dataset2+"**")
                    bin_content = round(tD_histo.GetBinContent(tD_histo.GetNbinsX(), jj),2)
                    if bin_content > 5: bin_content = round(tD_histo.GetBinContent(tD_histo.GetNbinsX(), jj),0)
                    triggerDataset_histo[inumber].SetBinContent(iii, jnumber, bin_content)
   

if opts.newDataset == "yes":                 
    newtD_histo=root_file.Get("trigger_newDataset_corr")
    newdD_histo=root_file.Get("newDataset_newDataset_corr")
    
    triggerNewDataset_histo=[]
    icount=-1
    nxentries = len(good_datasets)+1-4
    for i in range(0,newtD_histo.GetNbinsX()):
        dataset = newtD_histo.GetXaxis().GetBinLabel(i+1)
        if not newDatasetOK(dataset, newDatasetMap): continue
        nbinsY=0
        triggerList=[]
        for j in range(1,newtD_histo.GetNbinsY()+1):
            trigger = newtD_histo.GetYaxis().GetBinLabel(j)
            if not trigger in datasets: continue
            appendTrigger = False
            if physicsStreamOK(trigger):
                if dataset in datasets[trigger]:
                    appendTrigger = True
                else:
                    for old_dataset in list(newDatasetMap.keys()):
                        if not (dataset in newDatasetMap[old_dataset]): continue
                        if old_dataset in datasets[trigger]:
                            appendTrigger = True
                            break
            if appendTrigger:
                nbinsY += 1
                triggerList.append(trigger)
        if (nbinsY <= 0): continue
        n = nbinsY//max_y_entries + 1
        equi_div = nbinsY//n + 1
        if (nbinsY > max_y_entries):
            for nn in range(0,n):
                nnbinsY = 1
                if (nn == n-1):
                    nnbinsY = nbinsY-equi_div*nn
                else:
                    nnbinsY = equi_div
                triggerNewDataset_histo.append(ROOT.TH2F("newtD_"+dataset+str(nn),"Trigger-Dataset Overlap Rates (Hz)",nxentries,0,nxentries,nnbinsY,0,nnbinsY))
                icount+=1
        else:
            triggerNewDataset_histo.append(ROOT.TH2F("newtD_"+dataset,"Trigger-Dataset Overlap Rates (Hz)",nxentries,0,nxentries,nbinsY,0,nbinsY))
            icount+=1
    
        #Fill plots with only the "interesting" datasets and paths
        jjj=0
        for jj in range(1,newtD_histo.GetNbinsY()+1):
                trigger = newtD_histo.GetYaxis().GetBinLabel(jj)
                if not trigger in triggerList: continue
                jjj+=1
    
                n = nbinsY//max_y_entries
                nn = (jjj-1)//equi_div
                inumber=icount - n + nn
                jnumber=jjj-nn*equi_div
    
                iii=0
                for ii in range(1,newtD_histo.GetNbinsX()):
                    dataset2 = newtD_histo.GetXaxis().GetBinLabel(ii)
                    if not newDatasetOK(dataset2, newDatasetMap): continue
                    iii+=1
                    triggerNewDataset_histo[inumber].GetXaxis().SetBinLabel(iii, dataset2)
    
                    y_bin_label = trigger
                    if len(trigger) > max_y_label_length: y_bin_label = trigger[:max_y_label_length]
                    triggerNewDataset_histo[inumber].GetYaxis().SetBinLabel(jnumber, y_bin_label)
                    bin_content = round(newtD_histo.GetBinContent(ii, jj),2)
                    if bin_content > 5: bin_content = round(newtD_histo.GetBinContent(ii, jj),0)
                    triggerNewDataset_histo[inumber].SetBinContent(iii, jnumber, bin_content)
    
                    doIt = False
                    if dataset2 in datasets[trigger]:
                        doIt = True
                    else:
                        for old_dataset in list(newDatasetMap.keys()):
                            if dataset2 in newDatasetMap[old_dataset]:
                                if old_dataset in datasets[trigger]: doIt = True
                                break
                    if doIt:
                        iii+=1
                        triggerNewDataset_histo[inumber].GetXaxis().SetBinLabel(iii, dataset2+"**")
                        bin_content = round(newtD_histo.GetBinContent(newtD_histo.GetNbinsX(), jj),2)
                        if bin_content > 5: bin_content = round(newtD_histo.GetBinContent(newtD_histo.GetNbinsX(), jj),0)
                        triggerNewDataset_histo[inumber].SetBinContent(iii, jnumber, bin_content)
                    


for icount in range(0, len(triggerDataset_histo)):            
    print(triggerDataset_histo[icount].GetName())
    left_margin=0.45
    height=0.25*900+0.75*900*triggerDataset_histo[icount].GetNbinsY()/max_y_entries
    bottom_margin=0.15*900/height
    top_margin=0.1*900/height
    c_tD=ROOT.TCanvas("canvas_tD","",0,0,900,int(height))
    c_tD.SetGrid()
    c_tD.SetLeftMargin(left_margin)
    c_tD.SetRightMargin(0.12)
    c_tD.SetBottomMargin(bottom_margin)
    c_tD.SetTopMargin(top_margin)
    c_tD.cd()
    triggerDataset_histo[icount].SetMarkerColor(color)
    triggerDataset_histo[icount].SetMarkerSize(0.65)
    triggerDataset_histo[icount].GetYaxis().SetLabelSize(label_size)
    triggerDataset_histo[icount].GetXaxis().SetLabelSize(label_size)
    triggerDataset_histo[icount].GetXaxis().LabelsOption("v")
    triggerDataset_histo[icount].Draw("COLZTEXT")
    c_tD.SaveAs("%s/%s.pdf"%(outputDir, triggerDataset_histo[icount].GetName()))
    c_tD.Close()


if opts.newDataset == "yes":
    for icount in range(0, len(triggerNewDataset_histo)):            
        print(triggerNewDataset_histo[icount].GetName())
        left_margin=0.45
        height=0.25*900+0.75*900*triggerNewDataset_histo[icount].GetNbinsY()/max_y_entries
        bottom_margin=0.15*900/height
        top_margin=0.1*900/height
        c_tD=ROOT.TCanvas("canvas_tD","",0,0,900,int(height))
        c_tD.SetGrid()
        c_tD.SetLeftMargin(left_margin)
        c_tD.SetRightMargin(0.12)
        c_tD.SetBottomMargin(bottom_margin)
        c_tD.SetTopMargin(top_margin)
        c_tD.cd()
        triggerNewDataset_histo[icount].SetMarkerColor(color)
        triggerNewDataset_histo[icount].SetMarkerSize(0.65)
        triggerNewDataset_histo[icount].GetYaxis().SetLabelSize(label_size)
        triggerNewDataset_histo[icount].GetXaxis().SetLabelSize(label_size)
        triggerNewDataset_histo[icount].GetXaxis().LabelsOption("v")
        triggerNewDataset_histo[icount].Draw("COLZTEXT")
        c_tD.SaveAs("%s/%s.pdf"%(outputDir, triggerNewDataset_histo[icount].GetName()))
        c_tD.Close()


datasetDataset_histo=ROOT.TH2F("dD","Dataset-Dataset Overlap Rates (Hz)",len(good_datasets),0,len(good_datasets),len(good_datasets),0,len(good_datasets))
kk=0
for k in range(1,dD_histo.GetNbinsX()+1):
    dataset1 = dD_histo.GetXaxis().GetBinLabel(k)
    if not datasetOK(dataset1): continue
    kk+=1
    datasetDataset_histo.GetXaxis().SetBinLabel(kk, dataset1)

    ll=0
    for l in range(1,dD_histo.GetNbinsY()+1):
        dataset2 = dD_histo.GetYaxis().GetBinLabel(l)
        if not datasetOK(dataset2): continue
        ll+=1
        datasetDataset_histo.GetYaxis().SetBinLabel(ll, dataset2)
        bin_content=round(dD_histo.GetBinContent(k,l),2)
        if bin_content > 5: bin_content = round(dD_histo.GetBinContent(k, l),0)
        datasetDataset_histo.SetBinContent(kk, ll, bin_content)
                            
datasetDataset_histo.SetMarkerColor(color)
datasetDataset_histo.SetMarkerSize(0.7)
datasetDataset_histo.GetYaxis().SetLabelSize(0.022)
datasetDataset_histo.GetXaxis().SetLabelSize(0.022)
datasetDataset_histo.GetXaxis().LabelsOption("v")

c_dD.cd()
datasetDataset_histo.Draw("COLZTEXT")
c_dD.SaveAs("%s/datasetDataset_corr.pdf"%outputDir)
c_dD.Close()

c_newdD=ROOT.TCanvas("canvas_newdD","",0,0,900,900)
c_newdD.SetGrid()
c_newdD.SetLeftMargin(0.19)
c_newdD.SetRightMargin(0.12)
c_newdD.SetBottomMargin(0.19)


if opts.newDataset == "yes":
    newDatasetNewDataset_histo=ROOT.TH2F("newdD","New Dataset-Dataset Overlap Rates (Hz)",nxentries-1,0,nxentries-1,nxentries-1,0,nxentries-1)
    kk=0
    for k in range(1,newdD_histo.GetNbinsX()+1):
        dataset1 = newdD_histo.GetXaxis().GetBinLabel(k)
        print(dataset1)
        if not newDatasetOK(dataset1, newDatasetMap): continue
        kk+=1
        newDatasetNewDataset_histo.GetXaxis().SetBinLabel(kk, dataset1)
    
        ll=0
        for l in range(1,newdD_histo.GetNbinsY()+1):
            dataset2 = newdD_histo.GetYaxis().GetBinLabel(l)
            if not newDatasetOK(dataset2, newDatasetMap): continue
            ll+=1
            newDatasetNewDataset_histo.GetYaxis().SetBinLabel(ll, dataset2)
            bin_content=round(newdD_histo.GetBinContent(k,l),2)
            if bin_content > 5: bin_content = round(newdD_histo.GetBinContent(k, l),0)
            newDatasetNewDataset_histo.SetBinContent(kk, ll, bin_content)
                                
    newDatasetNewDataset_histo.SetMarkerColor(color)
    newDatasetNewDataset_histo.SetMarkerSize(0.7)
    newDatasetNewDataset_histo.GetYaxis().SetLabelSize(0.022)
    newDatasetNewDataset_histo.GetXaxis().SetLabelSize(0.022)
    newDatasetNewDataset_histo.GetXaxis().LabelsOption("v")
    
    c_newdD.cd()
    newDatasetNewDataset_histo.Draw("COLZTEXT")
    c_newdD.SaveAs("%s/newDatasetNewDataset_corr.pdf"%outputDir)
    c_newdD.Close()
