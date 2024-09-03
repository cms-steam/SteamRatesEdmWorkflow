#!/bin/bash

# Directories containing the ROOT files
TAR_DIR="/eos/cms/store/group/dpg_trigger/comm_trigger/TriggerStudiesGroup/STEAM/AlCa_Val/CMSALCA-293-test/Tar"
REF_DIR="/eos/cms/store/group/dpg_trigger/comm_trigger/TriggerStudiesGroup/STEAM/AlCa_Val/CMSALCA-293-test/Ref"

# Loop over each ROOT file in the Tar directory
for file in "$TAR_DIR"/hlt*.root; do
  # Get the number of events in the file using edmFileUtil
  event_count=$(edmFileUtil "$file" 2>/dev/null | grep -oP '(?<=, )\d+(?= events)')

  # Extract the filename without the directory path
  filename=$(basename "$file")

  # Check if event_count is not empty and if the number of events is less than 5
  if [[ -n "$event_count" && "$event_count" -lt 5 ]]; then
    echo "File with fewer than 5 events detected: $file (Events: $event_count)"
    # Remove the file from Tar directory
    rm "$file"
    echo "Removed from Tar: $file"
    # Remove the corresponding file from Ref directory
    if [[ -f "$REF_DIR/$filename" ]]; then
      rm "$REF_DIR/$filename"
      echo "Removed from Ref: $REF_DIR/$filename"
    else
      echo "No corresponding file in Ref directory: $REF_DIR/$filename"
    fi
  else
    if [[ -z "$event_count" ]]; then
      echo "Warning: Could not determine the number of events for $file. Skipping."
    else
      echo "File is good: $file (Events: $event_count)"
    fi
  fi
done

# Run hltDiff on all matching files in Tar and Ref directories
echo "Running hltDiff on remaining files:"
hltDiff -n "$TAR_DIR"/hlt*.root -o "$REF_DIR"/hlt*.root -c
