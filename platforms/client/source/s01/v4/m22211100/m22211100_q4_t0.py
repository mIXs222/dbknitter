import pymysql
import csv

# Database connection parameters
db_config = {
    "host": "mysql",
    "user": "root",
    "password": "my-secret-pw",
    "database": "tpch"
}

# Function to execute the query and fetch results
def fetch_results():
    # Construct the query
    query = """
    SELECT
        O_ORDERPRIORITY,
        COUNT(*) AS ORDER_COUNT
    FROM
        orders
    WHERE
        O_ORDERDATE >= '1993-07-01'
        AND O_ORDERDATE < '1993-10-01'
        AND EXISTS (
            SELECT
                *
            FROM
                lineitem
            WHERE
                L_ORDERKEY = O_ORDERKEY
                AND L_COMMITDATE < L_RECEIPTDATE
            )
    GROUP BY
        O_ORDERPRIORITY
    ORDER BY
        O_ORDERPRIORITY
    """

    # Connect to the MySQL database and execute the query
    connection = pymysql.connect(**db_config)
    try:
        with connection.cursor() as cursor:
            cursor.execute(query)
            results = cursor.fetchall()
    finally:
        connection.close()

    return results

# Write query results to a CSV file
def write_to_csv(results, filepath):
    with open(filepath, 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        # Write the header
        writer.writerow(['O_ORDERPRIORITY', 'ORDER_COUNT'])
        # Write the data rows
        writer.writerows(results)

# Main function to run the program logic
def main():
    results = fetch_results()
    write_to_csv(results, "query_output.csv")

if __name__ == "__main__":
    main()
