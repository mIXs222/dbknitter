import pymysql
import csv

# Define MySQL connection parameters
mysql_params = {
    'host': 'mysql',
    'user': 'root',
    'password': 'my-secret-pw',
    'database': 'tpch'
}

# Connect to the MySQL database
mysql_conn = pymysql.connect(**mysql_params)
mysql_cursor = mysql_conn.cursor()

# SQL query
sql_query = """
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

# Execute the query
mysql_cursor.execute(sql_query)

# Write the result to a CSV file
with open('query_output.csv', 'w', newline='') as csvfile:
    csvwriter = csv.writer(csvfile)
    # Write header
    csvwriter.writerow(['O_ORDERPRIORITY', 'ORDER_COUNT'])
    # Write data rows
    for row in mysql_cursor.fetchall():
        csvwriter.writerow(row)

# Close the cursor and MySQL connection
mysql_cursor.close()
mysql_conn.close()
