import ROOT
import math
from Menu_HLT import datasetMap as  triggersDatasetMap
from aux import streamOK
from aux import datasetOK
from aux import datasets_for_corr as good_datasets

label_size=0.022
max_y_entries = 40

c_dD=ROOT.TCanvas("canvas_dD","",0,0,900,900)
c_dD.SetGrid()
c_dD.SetLeftMargin(0.19)
c_dD.SetRightMargin(0.12)
c_dD.SetBottomMargin(0.19)


#some ROOT cosmetics
kRainBow=55
ROOT.gStyle.SetFrameLineWidth(3)
ROOT.gStyle.SetLineWidth(3)
ROOT.gStyle.SetOptStat(0)
ROOT.gStyle.SetPalette(kRainBow)


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
        print trigger
        if not trigger in triggersDatasetMap: continue
        if dataset in triggersDatasetMap[trigger] and streamOK(trigger):
            nbinsY += 1
            triggerList.append(trigger)
    if (nbinsY <= 0): continue
    if (nbinsY > max_y_entries):
        n = nbinsY//max_y_entries + 1
        for nn in range(0,n):
            nnbinsY = 1
            if (nn == n-1):
                nnbinsY = nbinsY-max_y_entries*nn
            else:
                nnbinsY = max_y_entries
            triggerDataset_histo.append(ROOT.TH2F("tD_"+dataset+str(nn),"Trigger-Dataset Overlap Rates (Hz)",len(good_datasets),0,len(good_datasets),nnbinsY,0,nnbinsY))
            icount+=1
    else:
        triggerDataset_histo.append(ROOT.TH2F("tD_"+dataset,"Trigger-Dataset Overlap Rates (Hz)",len(good_datasets),0,len(good_datasets),nbinsY,0,nbinsY))
        icount+=1

    #Fill plots with only the "interesting" datasets and paths
    jjj=0
    for jj in range(1,tD_histo.GetNbinsY()):
            trigger = tD_histo.GetYaxis().GetBinLabel(jj)
            if not trigger in triggerList: continue
            jjj+=1

            n = nbinsY//max_y_entries + 1
            nn = (jjj-1)//max_y_entries
            inumber=icount-(n-(nn+1))
            jnumber=jjj-nn*max_y_entries
            #print icount, inumber, jnumber

            iii=0
            for ii in range(1,tD_histo.GetNbinsX()+1):
                dataset2 = tD_histo.GetXaxis().GetBinLabel(ii)
                if not datasetOK(dataset2): continue
                iii+=1
                triggerDataset_histo[icount].GetXaxis().SetBinLabel(iii, dataset2)
                #print icount, inumber, jnumber, dataset2

                triggerDataset_histo[inumber].GetYaxis().SetBinLabel(jnumber, trigger)
                bin_content = tD_histo.GetBinContent(ii, jj)
                triggerDataset_histo[inumber].SetBinContent(iii, jnumber, bin_content)


for icount in range(0, len(triggerDataset_histo)):            
    left_margin=0.35
    bottom_margin=0.15
    c_tD=ROOT.TCanvas("canvas_tD","",0,0,850,1800)
    c_tD.SetGrid()
    c_tD.SetLeftMargin(left_margin)
    c_tD.SetRightMargin(0.12)
    c_tD.SetBottomMargin(bottom_margin)
    c_tD.cd()
    triggerDataset_histo[icount].GetYaxis().SetLabelSize(label_size)
    triggerDataset_histo[icount].GetXaxis().SetLabelSize(label_size)
    triggerDataset_histo[icount].GetXaxis().LabelsOption("v")
    triggerDataset_histo[icount].Draw("COLZ")
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
                            

datasetDataset_histo.GetYaxis().SetLabelSize(0.022)
datasetDataset_histo.GetXaxis().SetLabelSize(0.022)
datasetDataset_histo.GetXaxis().LabelsOption("v")

c_dD.cd()
datasetDataset_histo.Draw("COLZ")
c_dD.SaveAs("datasetDataset_corr.pdf")
c_dD.Close()
