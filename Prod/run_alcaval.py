import os
import subprocess
import json
import sys
import shutil
import argparse

def remove_existing_job_directories():
    # Remove any existing directories with Jobs_*
    for item in os.listdir("."):
        if os.path.isdir(item) and item.startswith("Jobs_"):
            try:
                shutil.rmtree(item)
                print(f"Removed existing directory: {item}")
            except Exception as e:
                print(f"Error removing directory {item}: {e}")

def copy_files_to_directory(config_name):
    # Define the configuration-specific directory
    config_dir = f"Jobs_{config_name}"
    
    # Copy all .py and .sh files to the new directory
    for file in os.listdir("."):
        if file.endswith(".py") or file.endswith(".sh"):
            shutil.copy(file, config_dir)

def replace_input_file_list(run_number):
    # File to modify
    cfg_file = "run_steamflow_cfg.py"
    
    # New import line
    new_import_line = f"from list_cff_Run{run_number} import inputFileNames\n"
    
    # Read the content of the file
    with open(cfg_file, 'r') as file:
        lines = file.readlines()
    
    # Replace the old import line
    with open(cfg_file, 'w') as file:
        for line in lines:
            if line.startswith("from list_cff_Run"):
                file.write(new_import_line)
            else:
                file.write(line)

    print(f"Replaced the import line in {cfg_file} with list_cff_Run{run_number}.py")

def create_input_file_list(run_number, era_name):
    # Initialize the proxy before querying DAS
    print("Initializing VOMS proxy...")
    voms_command = ["voms-proxy-init", "--voms", "cms", "--valid", "168:00"]
    subprocess.run(voms_command, check=True)
    
    # Parameters for the file list creation
    dataset_base = "/HLTPhysics"
    version = f"Run{era_name}-v1"
    raw_type = "RAW"
    output_file = f"list_cff_Run{run_number}.py"

    # Initialize the output file with the beginning of the Python list
    with open(output_file, 'w') as f:
        f.write("inputFileNames=[\n")

    # Correctly format the DAS query
    query = f"file dataset={dataset_base}/{version}/{raw_type} run={run_number}"
    print(f"Querying files for Run={run_number} in dataset {dataset_base}/{version}/{raw_type}")
    
    try:
        result = subprocess.run(['dasgoclient', '--query', query], capture_output=True, text=True)
        if result.returncode != 0:
            print(f"Error querying DAS: {result.stderr}")
            sys.exit(1)
        
        # Write the file paths to the Python list in the output file
        with open(output_file, 'a') as f:
            for line in result.stdout.strip().split('\n'):
                if line:
                    f.write(f"    '{line}',\n")
            f.write("]\n")  # Finalize the Python list

    except Exception as e:
        print(f"Failed to query DAS or write to {output_file}: {e}")
        sys.exit(1)

    # Check if the output file is empty
    with open(output_file, 'r') as f:
        contents = f.read().strip()
        if contents == "inputFileNames=[]":
            print(f"Warning: The file list {output_file} is empty.")
            print("Please check if your proxy was successfully initialized.")
        else:
            print(f"File list created in {output_file}")

    # Replace the input file list import line in run_steamflow_cfg.py
    replace_input_file_list(run_number)

def run_hlt_config(global_tag, config_name, output_base_dir, grun_menu):
    # Create the configuration-specific directory
    config_dir = f"Jobs_{config_name}"
    os.makedirs(config_dir, exist_ok=True)
    
    # Copy necessary files to the new directory
    copy_files_to_directory(config_name)
    
    # Change to the new directory
    os.chdir(config_dir)
    
    # Run hltGetConfiguration with the specified global tag and GRun menu
    hlt_command = f"hltGetConfiguration {grun_menu} --full --offline --no-output --data --process MYHLT --type GRun --prescale 2p0E34+ZeroBias+HLTPhysics --globaltag {global_tag} --max-events -1 > hlt.py"
    subprocess.run(hlt_command, shell=True, check=True)
    
    # Dump config file
    dump_command = f"edmConfigDump hlt.py > hlt_config.py"
    subprocess.run(dump_command, shell=True, check=True)
    
    # Ensure the correct global tag is applied
    with open(f"hlt_config.py", 'r') as file:
        config_data = file.read()
    if global_tag not in config_data:
        print(f"Error: The global tag {global_tag} was not properly applied in {config_name}.")
        sys.exit(1)
    
    # Get CMSSW src directory from environment variable
    cmssw_src_dir = os.path.join(os.environ.get('CMSSW_BASE', ''), 'src')
    
    # Define output directory for this configuration
    output_dir = os.path.join(output_base_dir, config_name)
    
    # Ensure the output directory exists
    os.makedirs(output_dir, exist_ok=True)
    
    # Submit jobs with n=1
    proxy_path = "/afs/cern.ch/user/s/savarghe/private/x509up_u137185"
    cms_condor_command = f"./cmsCondorData.py run_steamflow_cfg.py {cmssw_src_dir} {output_dir} -n 1 -q workday -p {proxy_path}"
    subprocess.run(cms_condor_command, shell=True, check=True)
    
    submit_command = "./sub_total.jobb"
    subprocess.run(submit_command, shell=True, check=True)
    
    print(f"Jobs for {config_name} with GT {global_tag} submitted to {output_dir}.")
    
    # Move back to the original directory
    os.chdir("..")

def main(config_file, run_number=None, era_name=None):
    # Create the input file list if run number and era name are provided
    if run_number and era_name:
        create_input_file_list(run_number, era_name)
    
    # Load configuration from JSON file
    with open(config_file, 'r') as f:
        config = json.load(f)
    
    # Extract base directory, global tags, GRun menu, and ALCA ticket number from JSON
    base_output_dir = config['base_output_dir']
    alca_ticket_number = f"CMSALCA-{config['alca_ticket_number']}"
    target_gt = config['target_gt']
    ref_gt = config['ref_gt']
    grun_menu = config['grun_menu']
    
    # Construct the full output directory path
    output_base_dir = os.path.join(base_output_dir, alca_ticket_number)
    
    # Ensure the base output directory exists
    os.makedirs(output_base_dir, exist_ok=True)
    
    # Remove any existing Jobs_* directories
    remove_existing_job_directories()
    
    # Run for the provided global tags
    run_hlt_config(target_gt, "Tar", output_base_dir, grun_menu)
    run_hlt_config(ref_gt, "Ref", output_base_dir, grun_menu)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run ALCA validation and optionally create input file list.")
    parser.add_argument("config_file", help="Path to the JSON configuration file.")
    parser.add_argument("--run", type=int, help="Specify the run number to create the input file list.")
    parser.add_argument("--era", type=str, help="Specify the era name (required if --run is provided).")
    
    args = parser.parse_args()
    
    if args.run and not args.era:
        print("Error: --era must be specified if --run is provided.")
        sys.exit(1)
    
    main(args.config_file, args.run, args.era)
