import mysql.connector
import csv
import pandas as pd

# Establish the connection
cnx = mysql.connector.connect(user='root', password='my-secret-pw',
                              host='mysql',
                              database='tpch')

# Create a cursor object
cursor = cnx.cursor()

# Execute the query
cursor.execute("""
    SELECT
        YEAR(O_ORDERDATE) AS O_YEAR,
        SUM(CASE WHEN NATION = 'INDIA' THEN VOLUME ELSE 0 END) / SUM(VOLUME) AS MKT_SHARE
    FROM
        (
        SELECT
            L_EXTENDEDPRICE * (1 - L_DISCOUNT) AS VOLUME,
            N2.N_NAME AS NATION,
            O_ORDERDATE
        FROM
            PART,
            SUPPLIER,
            LINEITEM,
            ORDERS,
            CUSTOMER,
            NATION n1,
            NATION n2,
            REGION
        WHERE
            P_PARTKEY = L_PARTKEY
            AND S_SUPPKEY = L_SUPPKEY
            AND L_ORDERKEY = O_ORDERKEY
            AND O_CUSTKEY = C_CUSTKEY
            AND C_NATIONKEY = N1.N_NATIONKEY
            AND N1.N_REGIONKEY = R_REGIONKEY
            AND R_NAME = 'ASIA'
            AND S_NATIONKEY = N2.N_NATIONKEY
            AND O_ORDERDATE BETWEEN '1995-01-01' AND '1996-12-31'
            AND P_TYPE = 'SMALL PLATED COPPER'
        ) AS ALL_NATIONS
    GROUP BY
        O_YEAR
    ORDER BY
        O_YEAR
""")

# Fetch all the rows
rows = cursor.fetchall()

# Write rows in a CSV file
with open('query_output.csv', 'w', newline='') as file:
    writer = csv.writer(file)
    writer.writerow(['O_YEAR', 'MKT_SHARE'])
    for row in rows:
        writer.writerow(row)

# Close the cursor and connection
cursor.close()
cnx.close()
