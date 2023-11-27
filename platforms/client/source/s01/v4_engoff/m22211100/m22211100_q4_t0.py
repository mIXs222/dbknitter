# query.py

import pymysql
import csv

# Connect to the MySQL database
connection = pymysql.connect(host='mysql', 
                             user='root', 
                             password='my-secret-pw', 
                             database='tpch', 
                             charset='utf8mb4',
                             cursorclass=pymysql.cursors.Cursor)

try:
    with connection.cursor() as cursor:
        # Execute the SQL query
        cursor.execute("""
        SELECT o.O_ORDERPRIORITY, COUNT(DISTINCT o.O_ORDERKEY) AS order_count
        FROM orders o
        JOIN lineitem l ON o.O_ORDERKEY = l.L_ORDERKEY
        WHERE o.O_ORDERDATE >= '1993-07-01'
        AND o.O_ORDERDATE < '1993-10-01'
        AND l.L_RECEIPTDATE > l.L_COMMITDATE
        GROUP BY o.O_ORDERPRIORITY
        ORDER BY o.O_ORDERPRIORITY;
        """)

        # Fetch the results
        results = cursor.fetchall()

        # Write results to CSV file
        with open('query_output.csv', mode='w', newline='') as file:
            writer = csv.writer(file)
            writer.writerow(['O_ORDERPRIORITY', 'order_count'])
            for row in results:
                writer.writerow(row)

finally:
    connection.close()
