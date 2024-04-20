#!/bin/bash

# Define the directory containing Job folders
job_directory="./Jobs"  # change this to your actual directory path

# Initialize a counter to track the number of errors found
error_count=0
file_missing_count=0

# Loop through each Job directory
for job_dir in "$job_directory"/Job_*; do
    if [ -d "$job_dir" ]; then  # Check if it is a directory
        error_file="$job_dir/hlt.stderr"  # Define the path to the error file
        if [ -f "$error_file" ]; then  # Check if the error file exists
            # Search for the phrases "segmentation violation" or "std::runtime_error"
            if grep -qE "segmentation violation|runtime error" "$error_file"; then
                echo "Error found in $error_file:"
                grep -E "segmentation violation|runtime error" "$error_file"  # Display the matching lines
                error_count=$((error_count + 1))  # Increment the error count
            fi
        else
            echo "Error file missing: $error_file"
            file_missing_count=$((file_missing_count + 1))  # Increment the file missing count
        fi
    fi
done

# Summary of findings
if [ "$error_count" -eq 0 ] && [ "$file_missing_count" -eq 0 ]; then
    echo "No errors found and all error files are present."
elif [ "$error_count" -eq 0 ]; then
    echo "No error messages found in any present error files."
fi

if [ "$file_missing_count" -ne 0 ]; then
    echo "Error files missing in $file_missing_count job directories."
fi
