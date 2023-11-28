# query.py
import pymysql
import csv
from datetime import datetime

# Establish a connection to the MySQL database
connection = pymysql.connect(host='mysql',
                             user='root',
                             password='my-secret-pw',
                             database='tpch')

try:
    with connection.cursor() as cursor:
        # Define the SQL query
        sql = """
        SELECT L_SHIPMODE,
            SUM(CASE WHEN O_ORDERPRIORITY IN ('1-URGENT', '2-HIGH') THEN 1 ELSE 0 END) AS HIGH_LINE_COUNT,
            SUM(CASE WHEN O_ORDERPRIORITY NOT IN ('1-URGENT', '2-HIGH') THEN 1 ELSE 0 END) AS LOW_LINE_COUNT
        FROM orders
        JOIN lineitem ON O_ORDERKEY = L_ORDERKEY
        WHERE L_SHIPMODE IN ('MAIL', 'SHIP') AND
              L_COMMITDATE < L_RECEIPTDATE AND
              L_SHIPDATE < L_COMMITDATE AND
              L_RECEIPTDATE BETWEEN '1994-01-01' AND '1994-12-31'
        GROUP BY L_SHIPMODE
        ORDER BY L_SHIPMODE ASC;
        """
        cursor.execute(sql)
        result = cursor.fetchall()

        # Write the results to a CSV file
        with open('query_output.csv', 'w', newline='') as csvfile:
            filewriter = csv.writer(csvfile)
            filewriter.writerow(["L_SHIPMODE", "HIGH_LINE_COUNT", "LOW_LINE_COUNT"])
            for row in result:
                filewriter.writerow(row)

finally:
    connection.close()
