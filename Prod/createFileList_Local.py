import os
import glob

output_file = "list_cff_Skim_2026_L1_v1_0_0_398683_PU7noQuad.py"

input_wildcard = "/eos/cms/store/group/tsg/STEAM/L1Skims_EZB_L1_2026_v1_0_0/398683_PU7_noQuad/*.root"

def create_file_list(input_wildcard, output_file):
    all_files = glob.glob(input_wildcard)
    
    with open(output_file,"w") as out:
        out.write("inputFileNames=[\n")
        for file in all_files:
            out.write("'"+ file.replace("\n","").replace("/eos/cms","") +"',\n")
        out.write("]")

   

create_file_list(input_wildcard, output_file)
