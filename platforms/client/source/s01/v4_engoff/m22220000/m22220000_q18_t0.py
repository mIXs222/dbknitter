# Filename: query_large_volume_customers.py
import pymysql
import csv

# Establish a connection to the MySQL database
connection = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    database='tpch'
)

# Create the query to execute
query = """
SELECT c.C_NAME, c.C_CUSTKEY, o.O_ORDERKEY, o.O_ORDERDATE, o.O_TOTALPRICE, SUM(l.L_QUANTITY) as total_quantity
FROM customer c
JOIN orders o ON c.C_CUSTKEY = o.O_CUSTKEY
JOIN lineitem l ON o.O_ORDERKEY = l.L_ORDERKEY
GROUP BY o.O_ORDERKEY
HAVING total_quantity > 300
ORDER BY c.C_NAME;
"""

# Execute the query and get the results
with connection.cursor() as cursor:
    cursor.execute(query)
    result = cursor.fetchall()

# Writing output to the file 'query_output.csv'
with open("query_output.csv", "w", newline='') as csvfile:
    csvwriter = csv.writer(csvfile)
    # Write the header
    csvwriter.writerow(['C_NAME', 'C_CUSTKEY', 'O_ORDERKEY', 'O_ORDERDATE', 'O_TOTALPRICE', 'TOTAL_QUANTITY'])
    # Write data rows
    for row in result:
        csvwriter.writerow(row)

# Close the connection
connection.close()
