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
import matplotlib.pyplot as plt
import csv

#Pie charts
group_label=[]
group_total=[]
group_pure=[]
group_pureplusshared=[]
colors=[]
basic_colors = ['gold', 'yellowgreen', 'lightcoral', 'lightskyblue', 'red', 'lightsalmon']
with open("output.group.csv") as group_file:
    reader=csv.reader(group_file, delimiter=',')
    skip = True #skip 1st line
    pureplusshared_count = 0
    for row in reader:
        if skip:
            skip = False
            continue
        pureplusshared_count += float(row[5])

    skip = True #skip 1st line
    others_total_count = 0
    others_pure_count = 0
    others_pureplusshared_count = 0

n=0
with open("output.group.csv") as group_file:
    reader=csv.reader(group_file, delimiter=',')
    for row in reader:
        if skip:
            skip = False
            continue
        if (float(row[5])/pureplusshared_count > 0.02):
            group_label.append(row[0])
            group_total.append(int(row[1]))
            group_pure.append(int(row[3]))
            group_pureplusshared.append(float(row[5]))
            colors.append(basic_colors[n % len(basic_colors)])
            n += 1
        else:
            others_total_count += int(row[1])
            others_pure_count += int(row[3])
            others_pureplusshared_count += float(row[5])

group_label.append('Others')
group_total.append(others_total_count)
group_pure.append(others_pure_count)
group_pureplusshared.append(others_pureplusshared_count)
n +=1
colors.append(basic_colors[n % len(basic_colors)])


plt.pie(group_total, labels=group_label, autopct='%1.1f%%', startangle=140, colors=colors)
plt.title('Group Rates')
plt.axis('equal')
plt.savefig("group_total.pdf")

plt.clf()
plt.pie(group_pure, labels=group_label, autopct='%1.1f%%', startangle=140, colors=colors)
plt.title('Pure Group Rates')
plt.axis('equal')
plt.savefig("group_pure.pdf")

plt.clf()
plt.pie(group_pureplusshared, labels=group_label, autopct='%1.1f%%', startangle=140, colors=colors)
plt.title('Pure+Shared Group Rates')
plt.axis('equal')
plt.savefig("group_pureplusshared.pdf")
    


#Overlap plots
label_size=0.022
max_y_entries = 40

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


#file=ROOT.TFile("final.root","r")
root_file=ROOT.TFile("corr_histos.root","R")

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
        if not trigger in triggersDatasetMap: continue
        if dataset in triggersDatasetMap[trigger] and physicsStreamOK(trigger):
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
            triggerDataset_histo.append(ROOT.TH2F("tD_"+dataset+str(nn),"Trigger-Dataset Overlap Rates (Hz)",len(good_datasets),0,len(good_datasets),nnbinsY,0,nnbinsY))
            icount+=1
    else:
        triggerDataset_histo.append(ROOT.TH2F("tD_"+dataset,"Trigger-Dataset Overlap Rates (Hz)",len(good_datasets),0,len(good_datasets),nbinsY,0,nbinsY))
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
            for ii in range(1,tD_histo.GetNbinsX()+1):
                dataset2 = tD_histo.GetXaxis().GetBinLabel(ii)
                if not datasetOK(dataset2): continue
                iii+=1
                triggerDataset_histo[inumber].GetXaxis().SetBinLabel(iii, dataset2)

                triggerDataset_histo[inumber].GetYaxis().SetBinLabel(jnumber, trigger)
                bin_content = tD_histo.GetBinContent(ii, jj)
                triggerDataset_histo[inumber].SetBinContent(iii, jnumber, bin_content)


for icount in range(0, len(triggerDataset_histo)):            
    print triggerDataset_histo[icount].GetName()
    left_margin=0.35
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
    triggerDataset_histo[icount].SetMarkerSize(0.7)
    triggerDataset_histo[icount].GetYaxis().SetLabelSize(label_size)
    triggerDataset_histo[icount].GetXaxis().SetLabelSize(label_size)
    triggerDataset_histo[icount].GetXaxis().LabelsOption("v")
    triggerDataset_histo[icount].Draw("COLZTEXT")
    c_tD.SaveAs(triggerDataset_histo[icount].GetName()+".pdf")
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
        bin_content=dD_histo.GetBinContent(k,l)
        datasetDataset_histo.SetBinContent(kk, ll, bin_content)
                            
datasetDataset_histo.SetMarkerColor(color)
datasetDataset_histo.SetMarkerSize(0.7)
datasetDataset_histo.GetYaxis().SetLabelSize(0.022)
datasetDataset_histo.GetXaxis().SetLabelSize(0.022)
datasetDataset_histo.GetXaxis().LabelsOption("v")

c_dD.cd()
datasetDataset_histo.Draw("COLZTEXT")
c_dD.SaveAs("datasetDataset_corr.pdf")
c_dD.Close()
