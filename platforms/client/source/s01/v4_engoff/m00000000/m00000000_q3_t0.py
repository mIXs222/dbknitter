import pymysql
import csv
from datetime import datetime

# Establish connection to the MySQL database
connection = pymysql.connect(host='mysql', user='root', password='my-secret-pw', db='tpch')

try:
    with connection.cursor() as cursor:
        # SQL query
        query = """
        SELECT
            o.O_ORDERPRIORITY,
            SUM(l.L_EXTENDEDPRICE * (1 - l.L_DISCOUNT)) AS revenue
        FROM
            orders o
        JOIN
            customer c ON o.O_CUSTKEY = c.C_CUSTKEY
        JOIN
            lineitem l ON o.O_ORDERKEY = l.L_ORDERKEY
        WHERE
            l.L_SHIPDATE > '1995-03-15'
            AND c.C_MKTSEGMENT = 'BUILDING'
        GROUP BY
            o.O_ORDERPRIORITY
        ORDER BY
            revenue DESC
        """

        # Execute the SQL query
        cursor.execute(query)
        results = cursor.fetchall()

        # Writing query results to CSV file
        with open('query_output.csv', 'w', newline='') as csvfile:
            csvwriter = csv.writer(csvfile)
            # Write the headers
            csvwriter.writerow(['O_ORDERPRIORITY', 'REVENUE'])
            # Write data rows
            for row in results:
                csvwriter.writerow(row)

finally:
    connection.close()
