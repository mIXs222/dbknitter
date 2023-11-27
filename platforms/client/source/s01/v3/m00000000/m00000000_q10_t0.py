import csv
import mysql.connector

# Establishing DB connection
mydb = mysql.connector.connect(
   host="mysql",
   user="root",
   password="my-secret-pw",
   database="tpch"
)
# SQL statement
mycursor = mydb.cursor()
sql = """
SELECT
    C_CUSTKEY,
    C_NAME,
    SUM(L_EXTENDEDPRICE * (1 - L_DISCOUNT)) AS REVENUE,
    C_ACCTBAL,
    N_NAME,
    C_ADDRESS,
    C_PHONE,
    C_COMMENT
FROM
    customer,
    orders,
    lineitem,
    nation
WHERE
    C_CUSTKEY = O_CUSTKEY
    AND L_ORDERKEY = O_ORDERKEY
    AND O_ORDERDATE >= '1993-10-01'
    AND O_ORDERDATE < '1994-01-01'
    AND L_RETURNFLAG = 'R'
    AND C_NATIONKEY = N_NATIONKEY
GROUP BY
    C_CUSTKEY,
    C_NAME,
    C_ACCTBAL,
    C_PHONE,
    N_NAME,
    C_ADDRESS,
    C_COMMENT
ORDER BY
    REVENUE, C_CUSTKEY, C_NAME, C_ACCTBAL DESC
"""

mycursor.execute(sql)

rows = mycursor.fetchall()

# Write to a CSV File:
with open('query_output.csv', 'w') as f:
    csv_writer = csv.writer(f)
    csv_writer.writerows(rows)

mydb.close()
