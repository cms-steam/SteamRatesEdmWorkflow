#python3 run_alcaval.py config.json

import os
import subprocess
import json
import sys
import shutil

def copy_files_to_directory(config_name):
    # Define the configuration-specific directory
    config_dir = f"Jobs_{config_name}"
    
    # Copy all .py and .sh files to the new directory
    for file in os.listdir("."):
        if file.endswith(".py") or file.endswith(".sh"):
            shutil.copy(file, config_dir)

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

def main(config_file):
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
    
    # Run for the provided global tags
    run_hlt_config(target_gt, "Tar", output_base_dir, grun_menu)
    run_hlt_config(ref_gt, "Ref", output_base_dir, grun_menu)

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python run_jobs.py <config_file.json>")
        sys.exit(1)
    
    config_file = sys.argv[1]
    main(config_file)
