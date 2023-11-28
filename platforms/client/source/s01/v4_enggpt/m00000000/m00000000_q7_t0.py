# query_exec.py
import pymysql
import csv
from datetime import datetime

# Connect to the MySQL database
connection = pymysql.connect(database='tpch', user='root', password='my-secret-pw', host='mysql')

try:
    with connection.cursor() as cursor:
        # Define the SQL query
        query = """
        SELECT
            sn.N_NAME as supplier_nation,
            cn.N_NAME as customer_nation,
            YEAR(L_SHIPDATE) as year,
            SUM(L_EXTENDEDPRICE * (1 - L_DISCOUNT)) as revenue
        FROM
            supplier S
            JOIN nation sn ON S.S_NATIONKEY = sn.N_NATIONKEY
            JOIN lineitem L ON S.S_SUPPKEY = L.L_SUPPKEY
            JOIN orders O ON L.L_ORDERKEY = O.O_ORDERKEY
            JOIN customer C ON O.O_CUSTKEY = C.C_CUSTKEY
            JOIN nation cn ON C.C_NATIONKEY = cn.N_NATIONKEY
        WHERE
            sn.N_NAME IN ('JAPAN', 'INDIA')
            AND cn.N_NAME IN ('JAPAN', 'INDIA')
            AND sn.N_NAME <> cn.N_NAME
            AND L_SHIPDATE BETWEEN '1995-01-01' AND '1996-12-31'
        GROUP BY
            supplier_nation,
            customer_nation,
            year
        ORDER BY
            supplier_nation,
            customer_nation,
            year;
        """
        
        # Execute the SQL query
        cursor.execute(query)
        result = cursor.fetchall()

        # Write result to query_output.csv
        with open('query_output.csv', 'w', newline='') as csvfile:
            csvwriter = csv.writer(csvfile)
            # Write the header
            csvwriter.writerow(['supplier_nation', 'customer_nation', 'year', 'revenue'])
            # Write the data
            for row in result:
                csvwriter.writerow(row)
finally:
    # Close the database connection
    connection.close()
