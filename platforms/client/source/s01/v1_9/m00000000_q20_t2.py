import MySQLdb
import csv

# Connect to the database
db = MySQLdb.connect(host="mysql", user="root", passwd="my-secret-pw", db="tpch")

# Create a cursor object
cursor = db.cursor()

#sql = "...." # the sql query goes here
sql = """SELECT
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
                P_NAME LIKE 'forest%%'
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
    S_NAME"""

# Execute the query
cursor.execute(sql)

# Fetch all the rows
rows = cursor.fetchall()

# Write to csv
with open('query_output.csv', 'w') as f:
    writer = csv.writer(f)
    writer.writerow([i[0] for i in cursor.description]) # write headers
    writer.writerows(rows)

# close the cursor and connection   
cursor.close()
db.close()
