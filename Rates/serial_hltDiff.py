# Calls hltDiff on output hlt_*.root files in two different directories and then merges the output
# Note: Will only work if the input files for these hlt.root files are the same in both jobs
#
# Author: Karim El Morabit 
# Date: 01.04.2026

import argparse
import csv
import glob
import json
import math
import os
import subprocess


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-o', '--old', dest='old_dir')
    parser.add_argument('-n', '--new', dest='new_dir')
    args = parser.parse_args()
    print(args)

    # get files to process. Only those that exist in both directories
    files_to_process = getFilesToProcess(args.old_dir, args.new_dir)
    
    # Do the actuall diffs
    print("do diffs")
    fields, result_dict = getHltDiff_CsvStyle(args.old_dir, args.new_dir, files_to_process)
    print("diffs done")
    if result_dict == {}:
        print("No results could be produced")
        exit(1)
    #print(result_dict)
    # calculate the missing numbers (fractions etc.)
    result_dict = calculate_result_numbers(result_dict)
    print("calculations done")
    # write the merged file
    print("final dict")
    #print(result_dict)
    writeHltDiff_CsvStyle(fields, result_dict)
    print("merged_hltDiff.csv written")


def getFilesToProcess(old_dir, new_dir):
    old_files = [os.path.basename(f) for f in glob.glob(old_dir+"/hlt_*.root")]
    new_files = [os.path.basename(f) for f in glob.glob(new_dir+"/hlt_*.root")]

    in_both = list(set(old_files).intersection(set(new_files)))
    only_old = list(set(old_files).difference(set(new_files)))
    only_new = list(set(new_files).difference(set(old_files)))
    print("skipping the following files because they only exist in the OLD path")
    print(only_old)
    print("skipping the following files because they only exist in the NEW path")
    print(only_new)
    print("processing "+str(len(in_both))+" files")
    return in_both

def getSingleFileHltDiff_json(old, new):
    command = "hltDiff -o " + old + " -n " + new + " -j --prescale -F tmp_diff"
    print(command)
    subprocess.call(command, shell=True)

def getSingleFileHltDiff_csv(old, new):
    command = "hltDiff -o " + old + " -n " + new + " -c --prescale -F tmp_diff"
    print(command)
    subprocess.call(command, shell=True)

def getHltDiff_CsvStyle(old_dir, new_dir, files_to_process):
    result_dict = {}
    fields = []
    for ifile, f in enumerate(files_to_process):
        try:
            getSingleFileHltDiff_csv(os.path.join(old_dir, f), os.path.join(new_dir, f))
        except:
            print("failed for file ", f)
        else:
            with open("tmp_diff_trigger.csv") as infile:
                reader = csv.reader(infile)
                for irow, row in enumerate(reader):
                    if irow==0:
                        if ifile == 0:
                            fields = row
                    else:
                        trigger_name = row[10]
                        if trigger_name not in result_dict:
                            result_dict[trigger_name] = {
                                'Total':0,
                                'Accepted OLD':0,
                                'Accepted NEW':0,
                                'Gained':0,
                                'Lost':0,
                                '|G|/A_N + |L|/AO':0,
                                'sigma(AN)+sigma(AO)':0,
                                'Changed':0,
                                'C/(T-AO)':0.0,
                                'sigma(T-AO)': 0,
                                'trigger': trigger_name
                            }
                        result_dict[trigger_name]['Total']+=int(row[0])
                        result_dict[trigger_name]['Accepted OLD']+=int(row[1])
                        result_dict[trigger_name]['Accepted NEW']+=int(row[2])
                        result_dict[trigger_name]['Gained']+=int(row[3])
                        result_dict[trigger_name]['Lost']+=int(row[4])
    #    if ifile>=0: break
    return fields, result_dict



def calculate_result_numbers(result_dict):
    for path in result_dict:
        result_dict[path]['|G|/A_N + |L|/AO'] = 0.0
        if result_dict[path]['Accepted NEW'] > 0 and result_dict[path]['Accepted OLD'] > 0:
            result_dict[path]['|G|/A_N + |L|/AO'] = (abs(result_dict[path]['Gained'])/(result_dict[path]['Accepted NEW']+1e-10) + abs(result_dict[path]['Lost'])/(result_dict[path]['Accepted OLD']+1e-10))*100.0
        result_dict[path]['sigma(AN)+sigma(AO)'] = (math.sqrt(result_dict[path]['Accepted NEW'])/(result_dict[path]['Accepted NEW'] + 1e-10) + math.sqrt(result_dict[path]['Accepted OLD'])/(result_dict[path]['Accepted OLD'] + 1e-10)) * 100.0
        result_dict[path]['Changed'] = 0
        result_dict[path]['C/(T-AO)'] = 0.0
        result_dict[path]['sigma(T-AO)'] = 100.0*(math.sqrt(result_dict[path]['Total'] - result_dict[path]['Accepted NEW'])/(result_dict[path]['Total'] - result_dict[path]['Accepted NEW'] + 1e-10) )
        
        result_dict[path]['|G|/A_N + |L|/AO'] = round(result_dict[path]['|G|/A_N + |L|/AO'], 2)
        result_dict[path]['sigma(AN)+sigma(AO)'] = round(result_dict[path]['sigma(AN)+sigma(AO)'], 2)
        result_dict[path]['sigma(T-AO)'] = round(result_dict[path]['sigma(T-AO)'], 2)

    return result_dict

def writeHltDiff_CsvStyle(fields, result_dict):
    with open("merged_hltDiff.csv","w") as outfile:
        writer = csv.DictWriter(outfile, fieldnames = fields)
        writer.writeheader()
        for path in result_dict:
            writer.writerow(result_dict[path])




if __name__ == '__main__':
    main()

