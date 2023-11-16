# python_code.py

import pymysql
import csv

# MySQL Connection Information
host = "mysql"
user = "root"
password = "my-secret-pw"
db = "tpch"

# Connect to MySQL Server
connection = pymysql.connect(host=host, user=user, password=password, db=db)

# Create Cursor Object
cursor = connection.cursor()

# SQL Query
query = """
    SELECT
        CNTRYCODE,
        COUNT(*) AS NUMCUST,
        SUM(C_ACCTBAL) AS TOTACCTBAL
    FROM
        (
        SELECT
            SUBSTR(C_PHONE, 1 , 2) AS CNTRYCODE,
            C_ACCTBAL
        FROM
            customer
        WHERE
            SUBSTR(C_PHONE , 1 , 2) IN
            ('20', '40', '22', '30', '39', '42', '21')
        AND C_ACCTBAL > (
                SELECT
                    AVG(C_ACCTBAL)
                FROM
                    customer
                WHERE
                    C_ACCTBAL > 0.00
                AND SUBSTR(C_PHONE , 1 , 2) IN
                ('20', '40', '22', '30', '39', '42', '21')
        )
        AND NOT EXISTS (
            SELECT
                *
            FROM
                orders
            WHERE
                O_CUSTKEY = C_CUSTKEY
            )
        ) AS CUSTSALE
    GROUP BY
        CNTRYCODE
    ORDER BY
        CNTRYCODE
"""

# Execute SQL Query
cursor.execute(query)

# Fetch Result
result = cursor.fetchall()

# Write Result to CSV
with open('query_output.csv', 'w', newline='') as f:
    writer = csv.writer(f)
    writer.writerow(['CNTRYCODE', 'NUMCUST', 'TOTACCTBAL'])
    writer.writerows(result)

# Close Connection
connection.close()
