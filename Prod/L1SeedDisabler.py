# Simple script to disable required L1 seeds. To run $python3 L1SeedDisabler.py (the hlt config file should named hlt.py)
import fileinput
import os
text = "hlt.py"       

match = {
'L1_SingleLooseIsoEG26er1p5':'L1_SingleMuCosmics', 
'L1_SingleLooseIsoEG28er2p5':'L1_SingleMuCosmics', 
'L1_SingleLooseIsoEG28er2p1':'L1_SingleMuCosmics', 
'L1_SingleLooseIsoEG28er1p5':'L1_SingleMuCosmics', 
'L1_SingleLooseIsoEG30er2p5':'L1_SingleMuCosmics', 
'L1_SingleIsoEG28er2p5':'L1_SingleMuCosmics', 
'L1_SingleIsoEG28er2p1':'L1_SingleMuCosmics',  
'L1_SingleIsoEG28er1p5':'L1_SingleMuCosmics', 
'L1_SingleIsoEG30er2p5':'L1_SingleMuCosmics',
'L1_DoubleEG_LooseIso22_12_er2p5':'L1_SingleMuCosmics', 
'L1_LooseIsoEG26er2p1_HTT100er':'L1_SingleMuCosmics',  
'L1_DoubleIsoTau32er2p1':'L1_SingleMuCosmics',   
'L1_SingleJet180er2p5':'L1_SingleMuCosmics',  
'L1_SingleJet180':'L1_SingleMuCosmics',
'L1_DoubleJet150er2p5':'L1_SingleMuCosmics', 
'L1_DoubleJet30er2p5_Mass_Min300_dEta_Max1p5':'L1_SingleMuCosmics',
'L1_Mass_Min300_dEta_Max1p5':'L1_SingleMuCosmics', 
'L1_DoubleJet_110_35_DoubleJet35_Mass_Min620':'L1_SingleMuCosmics',  
'L1_TripleJet_95_75_65_DoubleJet_75_65_er2p5':'L1_SingleMuCosmics', 
'L1_HTT320er_QuadJet_70_55_40_40_er2p4':'L1_SingleMuCosmics',  
'L1_HTT360er':'L1_SingleMuCosmics', 
}

for line in fileinput.input(text, inplace=True):
    line = line.rstrip()
    if not line:
        continue
    for f_key, f_value in match.items():
        if f_key in line:
            line = line.replace(f_key, f_value)
    print(line)
    
import re


f = open("hlt.py","r")
data = f.read() # string of all file content

def replace_all(text, dic):
    for i, j in dic.items():
        text = re.sub(r"\b%s\b"%i, j, text) 
        # r"\b%s\b"% enables replacing by whole word matches only
    return text

data = replace_all(data,match)
print("L1 seed Disabling Done") 
