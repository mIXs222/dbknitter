# revenue_report.py

import pymysql
import csv
from datetime import datetime

# Establish connection to the MySQL database
connection = pymysql.connect(host='mysql',
                             user='root',
                             password='my-secret-pw',
                             db='tpch',
                             charset='utf8mb4',
                             cursorclass=pymysql.cursors.Cursor)

try:
    with connection.cursor() as cursor:
        # Write the SQL query to retrieve the data for the report
        query = """
        SELECT
            sn.N_NAME as supplier_nation,
            cn.N_NAME as customer_nation,
            YEAR(L_SHIPDATE) as year,
            SUM(L_EXTENDEDPRICE * (1 - L_DISCOUNT)) as revenue
        FROM
            supplier,
            lineitem,
            orders,
            customer,
            nation as sn,
            nation as cn
        WHERE
            S_SUPPKEY = L_SUPPKEY
            AND O_ORDERKEY = L_ORDERKEY
            AND C_CUSTKEY = O_CUSTKEY
            AND S_NATIONKEY = sn.N_NATIONKEY
            AND C_NATIONKEY = cn.N_NATIONKEY
            AND (
                (sn.N_NAME = 'JAPAN' AND cn.N_NAME = 'INDIA') OR
                (sn.N_NAME = 'INDIA' AND cn.N_NAME = 'JAPAN')
            )
            AND L_SHIPDATE BETWEEN '1995-01-01' AND '1996-12-31'
        GROUP BY supplier_nation, customer_nation, year
        ORDER BY supplier_nation, customer_nation, year ASC
        """

        # Execute the SQL query
        cursor.execute(query)
        rows = cursor.fetchall()

        # Write the outcome to a CSV file
        with open('query_output.csv', mode='w', newline='') as file:
            writer = csv.writer(file)
            # Write header row
            writer.writerow(['supplier_nation', 'customer_nation', 'year', 'revenue'])
            # Write all data rows
            for row in rows:
                writer.writerow(row)

finally:
    # Close the database connection
    connection.close()
