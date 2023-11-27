import pymysql
import csv
from datetime import datetime

# Connect to the MySQL database
conn = pymysql.connect(host='mysql', user='root', password='my-secret-pw', db='tpch')
cursor = conn.cursor()

# SQL query
sql_query = """
    SELECT l.L_SHIPMODE,
           CASE
               WHEN o.O_ORDERPRIORITY = '1-URGENT' OR o.O_ORDERPRIORITY = '2-HIGH'
                   THEN 'CRITICAL'
               ELSE 'OTHER'
           END AS PRIORITY,
           COUNT(*) AS LATE_LINEITEM_COUNT
    FROM lineitem l
    JOIN orders o ON l.L_ORDERKEY = o.O_ORDERKEY
    WHERE l.L_SHIPMODE IN ('MAIL', 'SHIP')
        AND l.L_RECEIPTDATE BETWEEN '1994-01-01' AND '1995-01-01'
        AND l.L_SHIPDATE < l.L_COMMITDATE
        AND l.L_RECEIPTDATE > l.L_COMMITDATE
    GROUP BY l.L_SHIPMODE, PRIORITY;
"""

cursor.execute(sql_query)

# Write output to CSV
with open('query_output.csv', mode='w', newline='') as file:
    csv_writer = csv.writer(file)
    csv_writer.writerow(['SHIP_MODE', 'PRIORITY', 'LATE_LINEITEM_COUNT'])
    for row in cursor:
        csv_writer.writerow(row)

# Close connection
cursor.close()
conn.close()
