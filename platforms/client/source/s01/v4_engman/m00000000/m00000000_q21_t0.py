# query.py
import pymysql
import csv

# Connection info
conn_config = {
    "database": "tpch",
    "user": "root",
    "password": "my-secret-pw",
    "host": "mysql",
}

# Connect to the MySQL server
conn = pymysql.connect(**conn_config)
cur = conn.cursor()

# Execute the query
query = """
SELECT COUNT(*) AS NUMWAIT, S_NAME
FROM (
    SELECT L_ORDERKEY, S_NAME
    FROM lineitem
    JOIN orders ON L_ORDERKEY = O_ORDERKEY
    JOIN supplier ON L_SUPPKEY = S_SUPPKEY
    JOIN nation ON supplier.S_NATIONKEY = nation.N_NATIONKEY
    WHERE N_NAME = 'SAUDI ARABIA'
    AND O_ORDERSTATUS = 'F'
    AND L_COMMITDATE < L_RECEIPTDATE
    AND L_RECEIPTDATE > '1995-01-01'
    GROUP BY L_ORDERKEY, S_NAME
    HAVING COUNT(DISTINCT S_SUPPKEY) = 1
    AND COUNT(DISTINCT L_SUPPKEY) > 1
) AS subquery
GROUP BY S_NAME
ORDER BY NUMWAIT DESC, S_NAME ASC;
"""

cur.execute(query)

# Write results to a CSV file
with open("query_output.csv", "w", newline="") as csvfile:
    writer = csv.writer(csvfile)
    writer.writerow(["NUMWAIT", "S_NAME"])  # header
    for row in cur.fetchall():
        writer.writerow(row)

# Close the connection
cur.close()
conn.close()
