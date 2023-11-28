import pymysql
import csv
from datetime import datetime

# Establish a connection to the MySQL database
connection = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    db='tpch',
    charset='utf8mb4',
    cursorclass=pymysql.cursors.Cursor
)

# SQL query to fetch the required data
query = """
SELECT
    c.C_CUSTKEY,
    c.C_NAME,
    SUM(l.L_EXTENDEDPRICE * (1 - l.L_DISCOUNT)) AS REVENUE,
    c.C_ACCTBAL,
    n.N_NAME,
    c.C_ADDRESS,
    c.C_PHONE,
    c.C_COMMENT
FROM
    customer c
JOIN orders o ON c.C_CUSTKEY = o.O_CUSTKEY
JOIN lineitem l ON o.O_ORDERKEY = l.L_ORDERKEY
JOIN nation n ON c.C_NATIONKEY = n.N_NATIONKEY
WHERE
    o.O_ORDERDATE BETWEEN '1993-10-01' AND '1993-12-31'
    AND l.L_RETURNFLAG = 'R'
GROUP BY
    c.C_CUSTKEY, c.C_NAME, c.C_ACCTBAL, c.C_PHONE, n.N_NAME, c.C_ADDRESS, c.C_COMMENT
ORDER BY
    REVENUE ASC, c.C_CUSTKEY, c.C_NAME, c.C_ACCTBAL DESC;
"""

try:
    # Execute the SQL query
    with connection.cursor() as cursor:
        cursor.execute(query)
        results = cursor.fetchall()

        # Write results to CSV file
        with open('query_output.csv', 'w', newline='') as csvfile:
            writer = csv.writer(csvfile)
            # Write the header row
            writer.writerow(['C_CUSTKEY', 'C_NAME', 'REVENUE', 'C_ACCTBAL', 'N_NAME', 'C_ADDRESS', 'C_PHONE', 'C_COMMENT'])
            # Write the data rows
            for row in results:
                writer.writerow(row)

finally:
    # Close the connection to the database
    connection.close()
