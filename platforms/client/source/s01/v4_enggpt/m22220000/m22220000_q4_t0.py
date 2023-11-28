import pymysql
import csv
from datetime import datetime

# Establish connection to the MySQL database
connection = pymysql.connect(host='mysql', 
                             user='root', 
                             password='my-secret-pw', 
                             database='tpch', 
                             charset='utf8mb4', 
                             cursorclass=pymysql.cursors.Cursor)

try:
    with connection.cursor() as cursor:
        # Execute SQL query
        sql = """
        SELECT O_ORDERPRIORITY, COUNT(DISTINCT O_ORDERKEY) AS order_count
        FROM orders 
        INNER JOIN lineitem ON O_ORDERKEY = L_ORDERKEY
        WHERE O_ORDERDATE >= '1993-07-01' AND O_ORDERDATE < '1993-10-01'
          AND L_COMMITDATE < L_RECEIPTDATE
        GROUP BY O_ORDERPRIORITY
        ORDER BY O_ORDERPRIORITY ASC;
        """
        cursor.execute(sql)

        # Fetch all the rows
        rows = cursor.fetchall()

        # Write to file
        with open('query_output.csv', 'w', newline='') as csvfile:
            csvwriter = csv.writer(csvfile)
            csvwriter.writerow(['O_ORDERPRIORITY', 'order_count'])
            for row in rows:
                csvwriter.writerow(row)
finally:
    connection.close()
