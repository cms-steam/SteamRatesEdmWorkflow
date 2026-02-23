import os

dataset_list = [
"/ZeroBias/Run2025G-v1/RAW",
]

run_lumi_list={
    "398227": [[90,800]], 
}
output_file = "list_cff_ZB_398227.py"


def create_file_list(dataset_list, run_lumi_list, output_file):
    all_files = []
    for dataset in dataset_list:
        run_list = run_lumi_list.keys()
        for run in run_list:
            if run_lumi_list[run]==[[]]:
                query = '"file run = $RUN dataset=$DATASET"'.replace('$RUN',run).replace('$DATASET', dataset)
                print(query)
                os.system('dasgoclient -query='+query+' > tmp_list.txt')
            
                with open("tmp_list.txt","r") as tmp:
                    this_files = list(tmp)
                    all_files+=this_files
            else:
                for lr in run_lumi_list[run]:
                    lumi_list = [l for l in range(lr[0],lr[1]+1,1)]
                    for ls in lumi_list:
                        query = '"file run = $RUN dataset=$DATASET lumi=$LUMI"'.replace('$RUN',run).replace('$DATASET', dataset).replace('$LUMI',str(ls))
                        print(query)
                        os.system('dasgoclient -query='+query+' > tmp_list.txt')
                    
                        with open("tmp_list.txt","r") as tmp:
                            this_files = list(tmp)
                            all_files+=this_files
    
    with open(output_file,"w") as out:
        out.write("inputFileNames=[\n")
        for file in all_files:
            out.write("'"+ file.replace("\n","") +"',\n")
        out.write("]")

   
create_file_list(dataset_list, run_lumi_list, output_file)
