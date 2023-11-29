import pymysql
import csv

# Connecting to the MySQL database
connection_info = {
    'host': 'mysql',
    'user': 'root',
    'password': 'my-secret-pw',
    'db': 'tpch',
    'charset': 'utf8mb4'
}

# Define the query
query = """
SELECT
    o.O_ORDERPRIORITY,
    COUNT(*) AS ORDER_COUNT
FROM
    orders o
JOIN
    lineitem l ON o.O_ORDERKEY = l.L_ORDERKEY
WHERE
    o.O_ORDERDATE >= '1993-07-01'
    AND o.O_ORDERDATE < '1993-10-01'
    AND l.L_COMMITDATE < l.L_RECEIPTDATE
GROUP BY
    o.O_ORDERPRIORITY
ORDER BY
    o.O_ORDERPRIORITY
"""

try:
    # Connect to the database and execute query
    with pymysql.connect(**connection_info) as connection:
        with connection.cursor() as cursor:
            cursor.execute(query)
            results = cursor.fetchall()

    # Write the query results to a CSV file
    with open('query_output.csv', mode='w') as file:
        writer = csv.writer(file)
        writer.writerow(['ORDER_COUNT', 'O_ORDERPRIORITY'])  # Write header
        for row in results:
            writer.writerow(row)
finally:
    connection.close()
