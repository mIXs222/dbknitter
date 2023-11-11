#!/bin/bash

# Define the array of tables
tables=("nation" "region" "part" "supplier" "partsupp" "customer" "orders" "lineitem")

# Input string representing where tables are stored (0 for MongoDB, 1 for MySQL)
# This should be passed to the script as a single argument
storage_string="$1"

# Remove the brackets and extra spaces from the input string
storage_string="${storage_string//[\[\] ]/}"

# Split the string by comma into an array
IFS=',' read -r -a storage <<< "${storage_string}"

# Initialize variables to store the table names for MySQL and MongoDB
mysql_tables=()
mongodb_tables=()
redis_tables=()

# Iterate through the storage array and check if each element is a number
for i in "${!storage[@]}"; do
  if ! [[ "${storage[i]}" =~ ^[0-1]+$ ]]; then
    echo "Error: Each element in the storage array should be either '0' or '1'."
    exit 1
  fi

  if [[ "${storage[i]}" -eq 0 ]]; then
    # If the value is 0, add to MySQL tables
    mysql_tables+=("${tables[i]}")
  elif [[ "${storage[i]}" -eq 1 ]]; then
    # If the value is 1, add to MongoDB tables
    mongodb_tables+=("${tables[i]}")
  else  # elif [[ "${storage[i]}" -eq 2 ]]; then
    # If the value is 2, add to MongoDB tables
    redis_tables+=("${tables[i]}")
  fi
done

# Convert the arrays to comma-separated strings
if [ ${#mysql_tables[@]} -eq 0 ]; then
  mysql_tables_string="-"
else
  mysql_tables_string=$(IFS=,; echo "${mysql_tables[*]}")
fi

if [ ${#mongodb_tables[@]} -eq 0 ]; then
  mongodb_tables_string="-"
else
  mongodb_tables_string=$(IFS=,; echo "${mongodb_tables[*]}")
fi

if [ ${#redis_tables[@]} -eq 0 ]; then
  redis_tables_string="-"
else
  redis_tables_string=$(IFS=,; echo "${redis_tables[*]}")
fi


echo "MySQL: ${mysql_tables_string}, MongoDB: ${mongodb_tables_string}, Redis: ${redis_tables_string}"

# Run the command with the table names
bash cloudlab/tpch_init.sh /tpch/s01 ${mysql_tables_string} ${mongodb_tables_string} ${redis_tables_string}
