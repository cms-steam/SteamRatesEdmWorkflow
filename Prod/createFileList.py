import os

dataset_list = [
"/EphemeralHLTPhysics0/Run2025D-v1/RAW",
"/EphemeralHLTPhysics1/Run2025D-v1/RAW",
"/EphemeralHLTPhysics2/Run2025D-v1/RAW",
"/EphemeralHLTPhysics3/Run2025D-v1/RAW",
"/EphemeralHLTPhysics4/Run2025D-v1/RAW",
"/EphemeralHLTPhysics5/Run2025D-v1/RAW",
"/EphemeralHLTPhysics6/Run2025D-v1/RAW",
"/EphemeralHLTPhysics7/Run2025D-v1/RAW",
]

run_list = ["394959",]

output_file = "list_cff_Run3.py"


def create_file_list(dataset_list, run_list, output_file):
    all_files = []
    for dataset in dataset_list:
        for run in run_list:
            query = '"file run = $RUN dataset=$DATASET"'.replace('$RUN',run).replace('$DATASET', dataset)
            os.system('dasgoclient -query='+query+' > tmp_list.txt')
            
            with open("tmp_list.txt","r") as tmp:
                this_files = list(tmp)
                print(this_files)
#                print(len(this_files))
                all_files+=this_files
    
    with open(output_file,"w") as out:
        out.write("inputFileNames=[\n")
        for file in all_files:
            out.write("'"+ file.replace("\n","") +"',\n")
        out.write("]")

   

create_file_list(dataset_list, run_list, output_file)
