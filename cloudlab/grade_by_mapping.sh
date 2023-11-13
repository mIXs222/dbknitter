#!/bin/bash
int_handler() {
    echo "Interrupted."
    kill $PPID
    exit 1
}
trap 'int_handler' INT

if [ "$#" -ne 2 ]
then
  echo "Require 2 argument (MAPPING_STR, VERSION_DIR), $# provided"
  echo "Example: grade_by_mapping.sh 00001111 v1_0"
  echo "Example: grade_by_mapping.sh 00011122 v2"
  exit 1
fi

MAPPING_STR=$1
VERSION_DIR=$2
echo "Using MAPPING_STR=${MAPPING_STR}, VERSION_DIR=${VERSION_DIR}"

if [ "${#MAPPING_STR}" -ne 8 ]
then
  echo "MAPPING_STR requires 8 numbers: ${#MAPPING_STR} provided (${MAPPING_STR})."
  exit 1
fi
for (( i=0; i<${#MAPPING_STR}; i++ )); do
  mapping_char=${MAPPING_STR:$i:1}
  if ! [[ "${mapping_char}" =~ ^[0-2]+$ ]]; then
    echo "Error: Each element (${mapping_char}) in the MAPPING_STR array should be either '0', '1', or '2'."
    exit 1
  fi
done

output_dir="grade_output/${VERSION_DIR}/m${MAPPING_STR}"
mkdir -p ${output_dir}

# Start up
bash cloudlab/start-all.sh > ${output_dir}/docker.stdout 2> ${output_dir}/docker.stderr &
docker_pid=$!
echo "Starting Docker containers (PID= ${docker_pid})"
sleep 20
echo "Started Docker containers (PID= ${docker_pid})"

# Load tables
bash cloudlab/tpch_init_by_mapping.sh ${MAPPING_STR}
echo "Loaded TPC-H tables"

# Grade
(docker-compose -f cloudlab/docker-compose.yml exec client bash platform/run_grader.sh ${MAPPING_STR} ${VERSION_DIR}) | tee ${output_dir}/m${MAPPING_STR}.txt 
echo "Graded all sources"

# Stop docker
kill ${docker_pid}
bash cloudlab/clear-all.sh
sleep 5
echo "Graded all sources"