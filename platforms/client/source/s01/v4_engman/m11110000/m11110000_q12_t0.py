import pymysql
import csv
from datetime import datetime

# Connect to the MySQL database
connection = pymysql.connect(host='mysql',
                             user='root',
                             password='my-secret-pw',
                             database='tpch')

try:
    with connection.cursor() as cursor:
        # SQL query
        sql = """
        SELECT
            L_SHIPMODE,
            SUM(CASE WHEN O_ORDERPRIORITY = 'URGENT' OR O_ORDERPRIORITY = 'HIGH' THEN 1 ELSE 0 END) AS HIGH_PRIORITY_COUNT,
            SUM(CASE WHEN O_ORDERPRIORITY NOT IN ('URGENT', 'HIGH') THEN 1 ELSE 0 END) AS LOW_PRIORITY_COUNT
        FROM
            lineitem
        JOIN
            orders ON L_ORDERKEY = O_ORDERKEY
        WHERE
            L_RECEIPTDATE BETWEEN '1994-01-01' AND '1995-01-01'
            AND L_RECEIPTDATE > L_COMMITDATE
            AND L_SHIPMODE IN ('MAIL', 'SHIP')
            AND L_SHIPDATE < L_COMMITDATE
        GROUP BY
            L_SHIPMODE
        ORDER BY
            L_SHIPMODE
        """
        cursor.execute(sql)
        result = cursor.fetchall()

        # Write to CSV file
        with open('query_output.csv', 'w', newline='') as csvfile:
            writer = csv.writer(csvfile)
            # Write the header
            writer.writerow(['L_SHIPMODE', 'HIGH_PRIORITY_COUNT', 'LOW_PRIORITY_COUNT'])
            # Write the data
            for row in result:
                writer.writerow(list(row))
                
finally:
    connection.close()
