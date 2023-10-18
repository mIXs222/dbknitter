#!/bin/bash
# Database name
DATABASE_NAME="TPC-H.db"
# Directory containing the query files
QUERY_DIR="tpch-queries"
# Directory to store the result CSVs
RESULT_DIR="result"

# Create the result directory if it doesn't exist
if [ ! -d "$RESULT_DIR" ]; then
    mkdir "$RESULT_DIR"
fi

# Loop over each query file in the tcph-queries directory
for i in {1..22}; do
    QUERY_FILE="${QUERY_DIR}/q${i}.sql"
    OUTPUT_FILE="${RESULT_DIR}/q${i}.csv"
    
    # Check if the query file exists
    if [ -f "$QUERY_FILE" ]; then
        # Execute the query and save the result to a CSV
        sqlite3 "$DATABASE_NAME" <<EOF
.headers on
.mode csv
.output "$OUTPUT_FILE"
.read "$QUERY_FILE"
EOF
        echo "Executed query $i and saved results to $OUTPUT_FILE"
    else
        echo "Query file $QUERY_FILE does not exist"
    fi
done
