import pymysql
import csv

# Define database connection
conn = pymysql.connect(host='mysql', user='root', password='my-secret-pw', db='tpch')

# Create cursor
cur = conn.cursor()

# Execute sql query
cur.execute("""
SELECT
    C_COUNT,
    COUNT(*) AS CUSTDIST
FROM
    (
    SELECT
        C_CUSTKEY,
        COUNT(O_ORDERKEY) AS C_COUNT
    FROM
        customer LEFT OUTER JOIN orders ON
        C_CUSTKEY = O_CUSTKEY
        AND O_COMMENT NOT LIKE '%pending%deposits%'
    GROUP BY
        C_CUSTKEY
    )   C_ORDERS
GROUP BY
    C_COUNT
ORDER BY
    CUSTDIST DESC,
    C_COUNT DESC
""")

# Fetch all records
rows = cur.fetchall()

# Write all records to CSV
with open('query_output.csv', 'w', newline='') as f:
    writer = csv.writer(f)
    writer.writerow(['C_COUNT', 'CUSTDIST'])  # writing headers
    writer.writerows(rows)

# Close cursor and connection
cur.close()
conn.close()
