import pymysql
import csv
from datetime import datetime

# MySQL connection details
MYSQL_DB = 'tpch'
MYSQL_USER = 'root'
MYSQL_PASS = 'my-secret-pw'
MYSQL_HOST = 'mysql'

# Connect to MySQL
mysql_conn = pymysql.connect(host=MYSQL_HOST, user=MYSQL_USER, password=MYSQL_PASS, db=MYSQL_DB)
cursor = mysql_conn.cursor()

# Write the output to a CSV file
with open('query_output.csv', mode='w', newline='') as file:
    writer = csv.writer(file)
    
    # The header row
    writer.writerow(['l_shipmode', 'high_order_priority_count', 'low_order_priority_count'])
    
    try:
        # Execute query on MySQL
        mysql_query = """
        SELECT L_SHIPMODE,
               SUM(CASE WHEN O_ORDERPRIORITY = 'URGENT' OR O_ORDERPRIORITY = 'HIGH' THEN 1 ELSE 0 END) AS high_order_priority_count,
               SUM(CASE WHEN O_ORDERPRIORITY NOT IN ('URGENT', 'HIGH') THEN 1 ELSE 0 END) AS low_order_priority_count
        FROM lineitem
        JOIN orders ON L_ORDERKEY = O_ORDERKEY
        WHERE L_SHIPMODE IN ('MAIL', 'SHIP')
          AND L_RECEIPTDATE BETWEEN '1994-01-01' AND '1995-01-01'
          AND L_RECEIPTDATE > L_COMMITDATE
          AND L_SHIPDATE < L_COMMITDATE
        GROUP BY L_SHIPMODE
        ORDER BY L_SHIPMODE ASC;
        """
        cursor.execute(mysql_query)
        for row in cursor.fetchall():
            writer.writerow(row)
    finally:
        cursor.close()
        mysql_conn.close()
