import mysql.connector
import csv

# Connect to the database
mydb = mysql.connector.connect(
  host='mysql',
  user='root',
  password='my-secret-pw',
  database='tpch'
)

cursor = mydb.cursor()

# Define the SQL query
sql_query = """
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
"""

# Execute the SQL query
cursor.execute(sql_query)

# Fetch all rows from the last executed statement
rows = cursor.fetchall()

# Write the rows to CSV file
with open('query_output.csv', 'w', newline='', encoding='utf-8') as f:
    writer = csv.writer(f)
    writer.writerow([i[0] for i in cursor.description])  # write headers
    writer.writerows(rows)

# Close database connection
cursor.close()
mydb.close()
