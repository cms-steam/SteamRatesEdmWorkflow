#!/usr/bin/env python3

import os
import sys
import subprocess

def run_command(cmd, log_file=None):
    print(f"Running: {cmd}")
    if log_file:
        with open(log_file, 'w') as out:
            proc = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
            for line in proc.stdout:
                print(line, end='')
                out.write(line)
            proc.wait()
            if proc.returncode != 0:
                raise RuntimeError(f"Command failed: {cmd}")
    else:
        result = subprocess.run(cmd, shell=True)
        if result.returncode != 0:
            raise RuntimeError(f"Command failed: {cmd}")

def main():
    if len(sys.argv) != 2:
        print("Usage: ./make_MenuHLT.py <HLT_MENU_NAME>")
        print("Example: ./make_MenuHLT.py /dev/CMSSW_15_0_0/GRun/V48")
        sys.exit(1)

    hlt_menu = sys.argv[1]

    # Step 1: Get HLT config
    run_command(f"hltConfigFromDB --configName {hlt_menu} > hlt.py")

    # Step 2: Run hltDumpStream
    run_command("./hltDumpStream --csv --clean hlt.py", log_file="step1.log")

    # Step 3: Convert CSV to TSV
    run_command("python3 csv_to_tsv.py outputfile.csv steamdb.tsv")

    # Step 4: Generate maps
    run_command("python3 makeMaps.py steamdb.tsv > makeMaps.log")

    print("All steps completed successfully.")

if __name__ == "__main__":
    main()
