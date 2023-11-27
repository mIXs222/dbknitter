import pandas as pd
import pymysql
import csv

# Setup MySQL connection
db = pymysql.connect(host='mysql', user='root', password='my-secret-pw', db='tpch')

# Create a cursor
cursor = db.cursor()

# Write the SQL query
query = """
SELECT
    S_NAME,
    S_ADDRESS
FROM
    supplier,
    nation
WHERE
    S_SUPPKEY IN (
    SELECT
        PS_SUPPKEY
    FROM
        partsupp
    WHERE
        PS_PARTKEY IN (
            SELECT
                P_PARTKEY
            FROM
                part
            WHERE
                P_NAME LIKE 'forest%'
        )
        AND PS_AVAILQTY > (
            SELECT
                0.5 * SUM(L_QUANTITY)
            FROM
                lineitem
            WHERE
                L_PARTKEY = PS_PARTKEY
            AND L_SUPPKEY = PS_SUPPKEY
            AND L_SHIPDATE >= '1994-01-01'
            AND L_SHIPDATE < '1995-01-01'
        )
    )
    AND S_NATIONKEY = N_NATIONKEY
    AND N_NAME = 'CANADA'
ORDER BY
    S_NAME
"""

# Execute the query
cursor.execute(query)

# Fetch all the rows
rows = cursor.fetchall()

# Write to CSV file
with open('query_output.csv', 'w') as f:
    writer = csv.writer(f)
    for row in rows:
        writer.writerow(row)
