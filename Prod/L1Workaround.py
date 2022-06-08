import fileinput
import os
text = "hlt.py"

match = {
'L1_DoubleIsoTau26er2p1_Jet70_RmOvlp_dR0p5':'L1_SingleMuCosmics',
'L1_Mu18er2p1_Tau26er2p1_Jet70':'L1_SingleMuCosmics',
'L1_Mu18er2p1_Tau26er2p1_Jet55':'L1_SingleMuCosmics',
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
