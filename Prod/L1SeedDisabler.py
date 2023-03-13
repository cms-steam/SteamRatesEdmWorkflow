# Simple script to disable required L1 seeds. To run $python3 L1SeedDisabler.py (the hlt config file should named hlt.py)
import fileinput
import os
text = "hlt.py"       

match = {
'L1_HTT280er':'L1_HTT360er',
'L1_HTT320er':'L1_HTT360er',
'L1_HTT360er_QuadJet_70_55_40_35_er2p5': 'L1_HTT280er_QuadJet_70_55_40_35_er2p5',
'L1_HTT360er_QuadJet_70_55_40_40_er2p5': 'L1_HTT320er_QuadJet_70_55_40_40_er2p5',
'L1_HTT360er_QuadJet_80_60_er2p1_50_45_er2p3':'L1_HTT320er_QuadJet_80_60_er2p1_50_45_er2p3',
'L1_HTT360er_QuadJet_80_60_er2p1_45_40_er2p3':'L1_HTT320er_QuadJet_80_60_er2p1_45_40_er2p3',
'L1_DoubleMu5_SQ_OS_dR_Max1p6':'L1_SingleMuCosmics', 
'L1_DoubleMu3er2p0_SQ_OS_dR_Max1p6':'L1_SingleMuCosmics',
'L1_DoubleMu0er1p5_SQ_OS_dEta_Max1p2':'L1_SingleMuCosmics',
'L1_DoubleJet30er2p5_Mass_Min200_dEta_Max1p5':'L1_SingleMuCosmics',
'L1_DoubleJet30er2p5_Mass_Min225_dEta_Max1p5':'L1_SingleMuCosmics',
'L1_DoubleJet30er2p5_Mass_Min250_dEta_Max1p5':'L1_SingleMuCosmics',
'L1_DoubleIsoTau28er2p1_Mass_Max90':'L1_SingleMuCosmics', 
'L1_DoubleIsoTau28er2p1_Mass_Max80':'L1_SingleMuCosmics', 
'L1_DoubleIsoTau30er2p1_Mass_Max90':'L1_SingleMuCosmics', 
'L1_DoubleIsoTau30er2p1_Mass_Max80':'L1_SingleMuCosmics', 
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
