import csv
import pymysql
from datetime import datetime

# Connect to the MySQL database
connection = pymysql.connect(host='mysql',
                             user='root',
                             password='my-secret-pw',
                             db='tpch',
                             charset='utf8mb4',
                             cursorclass=pymysql.cursors.Cursor)

try:
    with connection.cursor() as cursor:
        # Execute the adapted SQL query for MySQL
        query = """
        SELECT
            N1.N_NAME AS SUPP_NATION,
            N2.N_NAME AS CUST_NATION,
            YEAR(L_SHIPDATE) AS L_YEAR,
            SUM(L_EXTENDEDPRICE * (1 - L_DISCOUNT)) AS REVENUE
        FROM
            supplier,
            lineitem,
            orders,
            customer,
            nation N1,
            nation N2
        WHERE
            supplier.S_SUPPKEY = lineitem.L_SUPPKEY
            AND orders.O_ORDERKEY = lineitem.L_ORDERKEY
            AND customer.C_CUSTKEY = orders.O_CUSTKEY
            AND supplier.S_NATIONKEY = N1.N_NATIONKEY
            AND customer.C_NATIONKEY = N2.N_NATIONKEY
            AND (
                (N1.N_NAME = 'JAPAN' AND N2.N_NAME = 'INDIA')
                OR (N1.N_NAME = 'INDIA' AND N2.N_NAME = 'JAPAN')
                )
            AND L_SHIPDATE BETWEEN '1995-01-01' AND '1996-12-31'
        GROUP BY
            SUPP_NATION,
            CUST_NATION,
            L_YEAR
        ORDER BY
            SUPP_NATION,
            CUST_NATION,
            L_YEAR
        """
        cursor.execute(query)

        # Fetch all the results
        results = cursor.fetchall()

        # Write the results to 'query_output.csv'
        with open('query_output.csv', mode='w', newline='') as file:
            csv_writer = csv.writer(file)
            
            # Write the header
            csv_writer.writerow(['SUPP_NATION', 'CUST_NATION', 'L_YEAR', 'REVENUE'])
            
            # Write the data
            for row in results:
                csv_writer.writerow(row)

finally:
    connection.close()
