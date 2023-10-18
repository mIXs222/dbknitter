# Check if the correct number of arguments are provided
if [ "$#" -ne 1 ]; then
    echo "Usage: $0 <query_file.sql>"
    exit 1
fi

# Database name
DATABASE_NAME="TPC-H.db"
# Directory to store the result CSVs
RESULT_DIR="result"

# Create the result directory if it doesn't exist
if [ ! -d "$RESULT_DIR" ]; then
    mkdir "$RESULT_DIR"
fi

# Extract filename without extension for the output CSV
BASENAME=$(basename "$1" .sql)
OUTPUT_FILE="${RESULT_DIR}/${BASENAME}.csv"

# Execute the query and save the result to a CSV
sqlite3 "$DATABASE_NAME" <<EOF
.headers on
.mode csv
.output "$OUTPUT_FILE"
.read "$1"
EOF

echo "Executed query and saved results to $OUTPUT_FILE"
