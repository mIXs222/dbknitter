import mysql.connector
import csv
import datetime

# Open database connection
db = mysql.connector.connect(user='root', password='my-secret-pw', host='mysql', database='tpch')

# prepare a cursor object using cursor() method
cursor = db.cursor()

# Execute the SQL command
cursor.execute("""
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
        AND P_NAME LIKE '%%dim%%'
""")

# Fetch all the rows in a list of lists.
results = cursor.fetchall()

results.sort(key=lambda x: (x[0], -x[1]))

# Write the results to a CSV file
with open('query_output.csv', 'w', newline='') as file:
    writer = csv.writer(file)
    writer.writerows(results)

# disconnect from server
db.close()
