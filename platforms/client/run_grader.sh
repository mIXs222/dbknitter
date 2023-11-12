#!/bin/bash
# Make sure we have exactly one argument
if [ "$#" -ne 2 ]; then
    echo "Usage: $0 config"
    exit 1
fi

config="$1"
gpt_result_directory="$2"
# Define directories based on the config parameter
expected_result_directory="/platform/expected/s01"
gpt_generated_source_code_directory="${gpt_result_directory}/m${config}"
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
    fname=$(basename $file)
    fbname=${fname%.*}
    query_number=$(echo "$file" | grep -oP '(?<=_q)\d+')
    # Construct the command as a string
    cmd="python $grader_file $file ${result_path}/${fbname}.csv ${expected_result_directory}/q${query_number}.csv"

# Echo the command so you can see it in the terminal
    echo $cmd

# Execute the command
    eval $cmd
done
