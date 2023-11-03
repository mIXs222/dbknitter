import mysql.connector
import csv

# Establish MySQL Connection
cnx = mysql.connector.connect(
  host="mysql",
  user="root",
  password="my-secret-pw",
  database="tpch"
)

# Create a cursor
cursor = cnx.cursor()

# Define the query
query = """
SELECT
    N_NAME,
    SUM(L_EXTENDEDPRICE * (1 - L_DISCOUNT)) AS REVENUE
FROM
    customer,
    orders,
    lineitem,
    supplier,
    nation,
    region
WHERE
    C_CUSTKEY = O_CUSTKEY
    AND L_ORDERKEY = O_ORDERKEY
    AND L_SUPPKEY = S_SUPPKEY
    AND C_NATIONKEY = S_NATIONKEY
    AND S_NATIONKEY = N_NATIONKEY
    AND N_REGIONKEY = R_REGIONKEY
    AND R_NAME = 'ASIA'
    AND O_ORDERDATE >= '1990-01-01'
    AND O_ORDERDATE < '1995-01-01'
GROUP BY
    N_NAME
ORDER BY
    REVENUE DESC
"""

# Execute the query
cursor.execute(query)

# Fetch all the rows
rows = cursor.fetchall()

# Write to CSV file
with open('query_output.csv', 'w', newline='') as f:
    csvwriter = csv.writer(f)
    csvwriter.writerow([i[0] for i in cursor.description])  # write headers
    csvwriter.writerows(rows)

# Close the cursor and connection
cursor.close()
cnx.close()
