#!/bin/bash

# Global result file
output_file="results.log"

# Clear the global result file if it exists
> "$output_file"

# Iterate through all first-level folders starting with 'experiment'
for first_level_folder in experiment*/; do
    # Write the first-level folder name to the output file
    first_level_folder_name=$(basename "$first_level_folder")
    echo "$first_level_folder_name" >> "$output_file"
    
    # Iterate through all subfolders in the first-level folder
    for subfolder in "$first_level_folder"*/; do
        # Count throughput
        tput=$(awk '/Throughput:/ {match($0, /Throughput: ([0-9.]+)/, a); sum += a[1]} END {print sum}' $subfolder/*.log)
        lat10=$(awk '/The 10p lat:/ {sum += $NF; count++} END {if (count > 0) print sum/count}' $subfolder/*.log)
        lat50=$(awk '/The 50p lat:/ {sum += $NF; count++} END {if (count > 0) print sum/count}' $subfolder/*.log)
        lat95=$(awk '/The 95p lat:/ {sum += $NF; count++} END {if (count > 0) print sum/count}' $subfolder/*.log)
        lat99=$(awk '/The 99p lat:/ {sum += $NF; count++} END {if (count > 0) print sum/count}' $subfolder/*.log)

        # Write the subfolder name and file count to the output file
        subfolder_name=$(basename "$subfolder")
        echo -e "$subfolder_name\t$tput\t$lat10\t$lat50\t$lat95\t$lat99" >> "$output_file"
    done
    
    # Add an empty line between different first-level folders
    echo "" >> "$output_file"
done

cat $output_file

echo "File counts have been written to $output_file"
