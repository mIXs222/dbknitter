import pymysql
import csv

# Define connection details
conn_details = {
    'host': 'mysql',
    'user': 'root',
    'password': 'my-secret-pw',
    'db': 'tpch',
}

# Connect to MySQL
conn = pymysql.connect(**conn_details)
cursor = conn.cursor()

# SQL Query
sql = """
SELECT
    SUM(L_EXTENDEDPRICE * L_DISCOUNT) AS REVENUE
FROM
    lineitem
WHERE
    L_SHIPDATE >= '1994-01-01'
    AND L_SHIPDATE < '1995-01-01'
    AND L_DISCOUNT BETWEEN .06 - 0.01 AND .06 + 0.01
    AND L_QUANTITY < 24
"""

# Execute the query
cursor.execute(sql)
result = cursor.fetchall()

# Write the output to a CSV file
with open('query_output.csv', 'w', newline='') as f:
    writer = csv.writer(f)
    writer.writerow(['REVENUE'])  # Header
    writer.writerows(result)

# Close the cursor and the connection
cursor.close()
conn.close()
