# query.py

import pymysql
import csv

# Connect to MYSQL
connection = pymysql.connect(host='mysql', user='root', password='my-secret-pw', database='tpch')
cursor = connection.cursor()

# SQL query for the MYSQL database
mysql_query = """
SELECT
    customer.C_CUSTKEY,
    customer.C_NAME,
    SUM(lineitem.L_EXTENDEDPRICE * (1 - lineitem.L_DISCOUNT)) AS REVENUE,
    customer.C_ACCTBAL,
    nation.N_NAME,
    customer.C_ADDRESS,
    customer.C_PHONE,
    customer.C_COMMENT
FROM
    customer
    JOIN orders ON customer.C_CUSTKEY = orders.O_CUSTKEY
    JOIN lineitem ON orders.O_ORDERKEY = lineitem.L_ORDERKEY
    JOIN nation ON customer.C_NATIONKEY = nation.N_NATIONKEY
WHERE
    orders.O_ORDERDATE >= '1993-10-01'
    AND orders.O_ORDERDATE < '1994-01-01'
    AND lineitem.L_RETURNFLAG = 'R'
GROUP BY
    customer.C_CUSTKEY,
    customer.C_NAME,
    customer.C_ACCTBAL,
    customer.C_PHONE,
    nation.N_NAME,
    customer.C_ADDRESS,
    customer.C_COMMENT
ORDER BY
    REVENUE DESC, customer.C_CUSTKEY, customer.C_NAME, customer.C_ACCTBAL;
"""

# Execute query
cursor.execute(mysql_query)

# Fetch all results
results = cursor.fetchall()

# Write results to CSV file
with open('query_output.csv', 'w', newline='') as csvfile:
    csvwriter = csv.writer(csvfile, delimiter=',')
    # Write the header
    csvwriter.writerow(['C_CUSTKEY', 'C_NAME', 'REVENUE', 'C_ACCTBAL', 'N_NAME', 'C_ADDRESS', 'C_PHONE', 'C_COMMENT'])
    # Write the data
    for row in results:
        csvwriter.writerow(row)

# Close cursor and connection
cursor.close()
connection.close()
