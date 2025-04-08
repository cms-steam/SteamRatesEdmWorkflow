import fileinput
import os
text = "hlt.py"
match = {
'L1_Mu3er1p5_Jet100er2p5_ETMHF30 OR L1_Mu3er1p5_Jet100er2p5_ETMHF40 OR L1_Mu3er1p5_Jet100er2p5_ETMHF50':'L1_Mu3er1p5_Jet100er2p5_ETMHF40 OR L1_Mu3er1p5_Jet100er2p5_ETMHF50',
'L1_DoubleMu3_SQ_ETMHF30_HTT60er OR L1_DoubleMu3_SQ_ETMHF40_HTT60er OR L1_DoubleMu3_SQ_ETMHF50_HTT60er OR L1_DoubleMu3_SQ_ETMHF30_Jet60er2p5_OR_DoubleJet40er2p5 OR L1_DoubleMu3_SQ_ETMHF40_Jet60er2p5_OR_DoubleJet40er2p5 OR L1_DoubleMu3_SQ_ETMHF50_Jet60er2p5_OR_DoubleJet40er2p5 OR L1_DoubleMu3_SQ_ETMHF50_Jet60er2p5 OR L1_DoubleMu3_SQ_ETMHF60_Jet60er2p5':'L1_DoubleMu3_SQ_ETMHF50_HTT60er OR L1_DoubleMu3_SQ_ETMHF50_Jet60er2p5_OR_DoubleJet40er2p5 OR L1_DoubleMu3_SQ_ETMHF50_Jet60er2p5 OR L1_DoubleMu3_SQ_ETMHF60_Jet60er2p5',
'L1_ETMHF80_SingleJet55er2p5_dPhi_Min2p1 OR L1_ETMHF90_SingleJet60er2p5_dPhi_Min2p1':'L1_ETMHF90_SingleJet60er2p5_dPhi_Min2p1',
'L1_ETMHF90_SingleJet60er2p5_dPhi_Min2p1':'L1_SingleMuCosmics',
'L1_ETMHF80_SingleJet55er2p5_dPhi_Min2p1':'L1_SingleMuCosmics',
'L1_ETMHF80_SingleJet55er2p5_dPhi_Min2p6':'L1_SingleMuCosmics',
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
print("Renaming Changes Undone") 
