#!/bin/bash
int_handler() {
    echo "Interrupted."
    kill $PPID
    exit 1
}

if [ "$#" -ne 2 ]; then
    echo "Example: bash run_grader.sh 00011122 v1_9"
    exit 1
fi

MAPPING_STR=$1
VERSION_DIR=$2
echo "Using MAPPING_STR=${MAPPING_STR}, VERSION_DIR=${VERSION_DIR}"

# Define directories based on the MAPPING_STR parameter
result_dir="/platform/expected/s01"
source_dir="/platform/source/s01/${VERSION_DIR}/m${MAPPING_STR}"
result_path="/platform/output/s01/${VERSION_DIR}/m${MAPPING_STR}"
grader_file="/dbknitter/dbknitter/grader.py"

# Create the result path with the correct permissions
mkdir -p "$result_path"
chmod 777 "$result_path"  # Setting permissions to 777; change as appropriate for your use case

# Run all the shell scripts in the gpt-generated source code directory using Docker

for script in ${source_dir}/*.sh; do
    echo "Running $script"
    bash "$script"
done

for source_file in ${source_dir}/*.py; do
    fname=$(basename $source_file)
    fbname=${fname%.*}
    query_number=$(echo "$source_file" | grep -oP '(?<=_q)\d+')
    # Construct the command as a string
    cmd="python $grader_file $source_file ${result_path}/${fbname}.csv ${result_dir}/q${query_number}.csv"

    # Echo the command so you can see it in the terminal
    echo $cmd

    # Execute the command
    eval $cmd
done
