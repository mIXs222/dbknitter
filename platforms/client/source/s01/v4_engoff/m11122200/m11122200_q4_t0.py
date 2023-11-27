# query.py

import pymysql
import csv

# Connection information
conn_info = {
    'host': 'mysql',
    'user': 'root',
    'password': 'my-secret-pw',
    'database': 'tpch',
    'charset': 'utf8mb4'
}

# Open a connection to the MySQL database
conn = pymysql.connect(**conn_info)
cursor = conn.cursor()

# Execute the query
query = """
SELECT
    O_ORDERPRIORITY,
    COUNT(*) AS order_count
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
            lineitem.L_ORDERKEY = orders.O_ORDERKEY
            AND lineitem.L_COMMITDATE < lineitem.L_RECEIPTDATE
    )
GROUP BY
    O_ORDERPRIORITY
ORDER BY
    O_ORDERPRIORITY;
"""

cursor.execute(query)

# Fetch the results
results = cursor.fetchall()

# Write the output to a CSV file
with open('query_output.csv', 'w', newline='') as csvfile:
    csvwriter = csv.writer(csvfile)
    csvwriter.writerow(['O_ORDERPRIORITY', 'order_count']) # header
    for row in results:
        csvwriter.writerow(row)

# Close the connection
cursor.close()
conn.close()
