import mysql.connector
import csv
import pandas as pd

# Establish the connection
cnx = mysql.connector.connect(user='root', password='my-secret-pw', host='mysql', database='tpch')

# Create a cursor object
cursor = cnx.cursor()

# Execute the query
query = """
SELECT
    NATION,
    O_YEAR,
    SUM(AMOUNT) AS SUM_PROFIT
FROM
    (
    SELECT
        N_NAME AS NATION,
        YEAR(O_ORDERDATE) AS O_YEAR,
        L_EXTENDEDPRICE * (1 - L_DISCOUNT) - PS_SUPPLYCOST * L_QUANTITY AS AMOUNT
    FROM
        PART,
        SUPPLIER,
        LINEITEM,
        PARTSUPP,
        ORDERS,
        NATION
    WHERE
        S_SUPPKEY = L_SUPPKEY
        AND PS_SUPPKEY = L_SUPPKEY
        AND PS_PARTKEY = L_PARTKEY
        AND P_PARTKEY = L_PARTKEY
        AND O_ORDERKEY = L_ORDERKEY
        AND S_NATIONKEY = N_NATIONKEY
        AND P_NAME LIKE '%dim%'
    ) AS PROFIT
GROUP BY
    NATION,
    O_YEAR
ORDER BY
    NATION,
    O_YEAR DESC
"""
cursor.execute(query)

# Fetch all the rows
rows = cursor.fetchall()

# Write to CSV
with open('query_output.csv', 'w', newline='') as f:
    writer = csv.writer(f)
    writer.writerows(rows)

# Close the connection
cnx.close()
