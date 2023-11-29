# query_execution.py
import pymysql
import csv
from datetime import datetime

# Establish a connection to the MySQL database
connection = pymysql.connect(host='mysql',
                             user='root',
                             password='my-secret-pw',
                             database='tpch',
                             charset='utf8mb4',
                             cursorclass=pymysql.cursors.Cursor)

try:
    with connection.cursor() as cursor:
        query = """
        SELECT l_shipmode, 
               SUM(CASE WHEN o_orderpriority = 'URGENT' OR o_orderpriority = 'HIGH' THEN 1 ELSE 0 END) AS high_priority_count,
               SUM(CASE WHEN o_orderpriority NOT IN ('URGENT', 'HIGH') THEN 1 ELSE 0 END) AS low_priority_count
        FROM lineitem
        JOIN orders ON l_orderkey = o_orderkey 
        WHERE l_receiptdate BETWEEN '1994-01-01' AND '1995-01-01'
          AND l_receiptdate > l_commitdate
          AND l_shipmode IN ('MAIL', 'SHIP')
          AND l_shipdate < l_commitdate
        GROUP BY l_shipmode
        ORDER BY l_shipmode ASC;
        """
        cursor.execute(query)
        results = cursor.fetchall()

        # Write query results to CSV
        with open('query_output.csv', 'w', newline='') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(['l_shipmode', 'high_priority_count', 'low_priority_count'])
            for row in results:
                writer.writerow(row)

finally:
    connection.close()
