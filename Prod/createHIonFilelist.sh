#!/bin/bash                                                                                                                                
# Parameters                                                                                                                               
run=375776
dataset_base="/HIEphemeralHLTPhysics"
version="HIRun2023A-v1"
raw_type="RAW"
output_file="list_cff_HIon.py"

# Initialize the output file with the beginning of the Python list                                                                         
echo "inputFileNames=[" > $output_file

# Query and append each file path to the Python list in the output file                                                                    
dasgoclient --query "file run=$run dataset=$dataset_base/$version/$raw_type" | while IFS= read -r line; do
    echo "    \"$line\"," >> $output_file
done

# Finalize the Python list in the output file                                                                                              
echo "]" >> $output_file

echo "File list created in $output_file"
