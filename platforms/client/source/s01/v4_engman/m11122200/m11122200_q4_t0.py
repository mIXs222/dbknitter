# query.py
import csv
import pymysql

# Connect to the MySQL database
conn = pymysql.connect(
    database="tpch",
    user="root",
    password="my-secret-pw",
    host="mysql",
)

# Create a cursor to execute the SQL query
cursor = conn.cursor()

# Define the SQL query
sql_query = """
SELECT COUNT(DISTINCT o.O_ORDERKEY) as ORDER_COUNT, o.O_ORDERPRIORITY
FROM orders o
JOIN lineitem l ON o.O_ORDERKEY = l.L_ORDERKEY
WHERE o.O_ORDERDATE >= '1993-07-01'
AND o.O_ORDERDATE < '1993-10-01'
AND l.L_RECEIPTDATE > l.L_COMMITDATE
GROUP BY o.O_ORDERPRIORITY
ORDER BY o.O_ORDERPRIORITY;
"""

# Execute the SQL query
cursor.execute(sql_query)

# Fetch all results
results = cursor.fetchall()

# Write results to CSV
with open('query_output.csv', 'w', newline='') as csvfile:
    csvwriter = csv.writer(csvfile)
    csvwriter.writerow(["ORDER_COUNT", "O_ORDERPRIORITY"])
    for row in results:
        csvwriter.writerow(row)

# Close the cursor and database connection
cursor.close()
conn.close()
