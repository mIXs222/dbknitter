# query.py
import pymysql
import csv

# Open database connection
conn = pymysql.connect(host='mysql', user='root', password='my-secret-pw', database='tpch')

# Create a cursor object using cursor() method
cursor = conn.cursor()

# SQL query
query = """
SELECT o.O_ORDERKEY,
       SUM(l.L_EXTENDEDPRICE * (1 - l.L_DISCOUNT)) AS REVENUE,
       o.O_ORDERDATE,
       o.O_SHIPPRIORITY
FROM orders o
JOIN customer c ON o.O_CUSTKEY = c.C_CUSTKEY
JOIN lineitem l ON l.L_ORDERKEY = o.O_ORDERKEY
WHERE c.C_MKTSEGMENT = 'BUILDING'
AND o.O_ORDERDATE < '1995-03-05'
AND l.L_SHIPDATE > '1995-03-15'
GROUP BY o.O_ORDERKEY, o.O_ORDERDATE, o.O_SHIPPRIORITY
ORDER BY REVENUE DESC;
"""

# Execute the SQL command
cursor.execute(query)

# Fetch all the rows
data = cursor.fetchall()

# Write data to CSV file
with open('query_output.csv', 'w', newline='') as csvfile:
    csvwriter = csv.writer(csvfile)
    # Write the header
    csvwriter.writerow(['O_ORDERKEY', 'REVENUE', 'O_ORDERDATE', 'O_SHIPPRIORITY'])
    # Write data rows
    for row in data:
        csvwriter.writerow(row)

# Close the cursor and connection
cursor.close()
conn.close()
