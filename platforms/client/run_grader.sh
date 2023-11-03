#!/bin/bash
###
 # @Author: University of Illinois at Urbana Champaign
 # @Date: 2023-11-02 21:07:38
 # @LastEditTime: 2023-11-03 14:22:32
 # @FilePath: /platforms/client/run_grader.sh
 # @Description: 
### 

# Make sure we have exactly one argument
if [ "$#" -ne 1 ]; then
    echo "Usage: $0 config"
    exit 1
fi

config="$1"

# Define directories based on the config parameter
expected_result_directory="/platforms/expected/s01"
gpt_generated_source_code_directory="/platform/source/s01/v1_9/m${config}"
result_path="/platform/output/m${config}"
grader_file="/grader.py"

# Create the result path with the correct permissions
mkdir -p "$result_path"
chmod 777 "$result_path"  # Setting permissions to 777; change as appropriate for your use case

# Run all the shell scripts in the gpt-generated source code directory using Docker

for script in ${gpt_generated_source_code_directory}/*.sh; do
    echo "Running $script"
    bash "$script"
done

for file in ${gpt_generated_source_code_directory}/*.py; do
    query_number=$(echo "$file" | grep -oP '(?<=_q)\d+')
    # Construct the command as a string
    cmd="python $grader_file $file ${result_path}/q${query_number}.csv ${expected_result_directory}/q${query_number}.csv"

# Echo the command so you can see it in the terminal
    echo $cmd

# Execute the command
    eval $cmd
done
