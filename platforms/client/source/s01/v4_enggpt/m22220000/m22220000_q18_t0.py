import pymysql
import csv

# Connect to the MySQL database
conn = pymysql.connect(host='mysql', user='root', password='my-secret-pw', db='tpch')
cursor = conn.cursor()

# Query to execute
query = """
SELECT c.C_NAME, c.C_CUSTKEY, o.O_ORDERKEY, o.O_ORDERDATE, o.O_TOTALPRICE, SUM(l.L_QUANTITY) AS total_quantity
FROM customer c
JOIN orders o ON c.C_CUSTKEY = o.O_CUSTKEY
JOIN lineitem l ON o.O_ORDERKEY = l.L_ORDERKEY
WHERE o.O_ORDERKEY IN (
    SELECT l.L_ORDERKEY
    FROM lineitem l
    GROUP BY l.L_ORDERKEY
    HAVING SUM(l.L_QUANTITY) > 300
)
GROUP BY c.C_NAME, c.C_CUSTKEY, o.O_ORDERKEY, o.O_ORDERDATE, o.O_TOTALPRICE
ORDER BY o.O_TOTALPRICE DESC, o.O_ORDERDATE ASC;
"""

# Execute the query
cursor.execute(query)

# Fetch all results
results = cursor.fetchall()

# Write the query results to the file
with open('query_output.csv', mode='w', newline='') as file:
    writer = csv.writer(file)
    writer.writerow(['C_NAME', 'C_CUSTKEY', 'O_ORDERKEY', 'O_ORDERDATE', 'O_TOTALPRICE', 'TOTAL_QUANTITY'])
    for row in results:
        writer.writerow(row)

# Close the cursor and the connection
cursor.close()
conn.close()
