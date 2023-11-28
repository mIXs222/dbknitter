# query.py

import pymysql
import csv
from datetime import datetime

# Connect to the MySQL server
connection = pymysql.connect(host='mysql',
                             user='root',
                             password='my-secret-pw',
                             database='tpch')

try:
    with connection.cursor() as cursor:
        # Construct the SQL query
        sql_query = """
        SELECT
            L_SHIPMODE,
            SUM(CASE WHEN O_ORDERPRIORITY = '1-URGENT' OR O_ORDERPRIORITY = '2-HIGH' THEN 1 ELSE 0 END) AS HIGH_LINE_COUNT,
            SUM(CASE WHEN O_ORDERPRIORITY <> '1-URGENT' AND O_ORDERPRIORITY <> '2-HIGH' THEN 1 ELSE 0 END) AS LOW_LINE_COUNT
        FROM
            orders
        INNER JOIN
            lineitem ON O_ORDERKEY = L_ORDERKEY
        WHERE
            L_SHIPMODE IN ('MAIL', 'SHIP')
            AND L_COMMITDATE < L_RECEIPTDATE
            AND L_SHIPDATE < L_COMMITDATE
            AND L_RECEIPTDATE BETWEEN '1994-01-01' AND '1994-12-31'
        GROUP BY
            L_SHIPMODE
        ORDER BY
            L_SHIPMODE ASC;
        """

        # Execute the SQL query
        cursor.execute(sql_query)

        # Write the results to a file
        with open("query_output.csv", "w", newline='') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(['L_SHIPMODE', 'HIGH_LINE_COUNT', 'LOW_LINE_COUNT'])  # header
            for row in cursor:
                writer.writerow(row)

finally:
    # Close the connection to the server
    connection.close()
