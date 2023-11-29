import pymysql
import csv
from datetime import datetime

# Connect to the MySQL database
connection = pymysql.connect(host='mysql',
                             user='root',
                             password='my-secret-pw',
                             database='tpch')

# SQL query
query = """
SELECT
      L_SHIPMODE,
      SUM(CASE WHEN O_ORDERPRIORITY = 'URGENT' OR O_ORDERPRIORITY = 'HIGH' THEN 1 ELSE 0 END) AS high_priority_count,
      SUM(CASE WHEN O_ORDERPRIORITY <> 'URGENT' AND O_ORDERPRIORITY <> 'HIGH' THEN 1 ELSE 0 END) AS low_priority_count
FROM
      lineitem
JOIN
      orders ON L_ORDERKEY = O_ORDERKEY
WHERE
      L_SHIPMODE IN ('MAIL', 'SHIP')
      AND L_RECEIPTDATE BETWEEN '1994-01-01' AND '1995-01-01'
      AND L_RECEIPTDATE > L_COMMITDATE
      AND L_SHIPDATE < L_COMMITDATE
GROUP BY
      L_SHIPMODE
ORDER BY
      L_SHIPMODE ASC;
"""

try:
    # Execute the SQL query
    with connection.cursor() as cursor:
        cursor.execute(query)
        results = cursor.fetchall()
        
        # Write to CSV
        with open('query_output.csv', 'w', newline='') as csvfile:
            writer = csv.writer(csvfile)
            # Write the header
            writer.writerow(['L_SHIPMODE', 'HIGH_PRIORITY_COUNT', 'LOW_PRIORITY_COUNT'])
            # Write the data
            for row in results:
                writer.writerow(row)

finally:
    # Close the connection
    connection.close()
