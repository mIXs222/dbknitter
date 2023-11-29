# query.py
import pymysql
import csv

# MySQL connection parameters
db_params = {
    "host": "mysql",
    "user": "root",
    "password": "my-secret-pw",
    "database": "tpch"
}

# Connect to the MySQL database
connection = pymysql.connect(**db_params)

# Create a cursor object
cursor = connection.cursor()

# The SQL query string
sql_query = """
SELECT SUM(L_EXTENDEDPRICE * L_DISCOUNT) AS REVENUE
FROM lineitem
WHERE L_SHIPDATE > '1994-01-01' AND L_SHIPDATE < '1995-01-01'
      AND L_DISCOUNT BETWEEN 0.06 - 0.01 AND 0.06 + 0.01
      AND L_QUANTITY < 24;
"""

# Execute the query
cursor.execute(sql_query)

# Fetch the result
result = cursor.fetchone()

# Write the result to a CSV file
with open("query_output.csv", "w", newline='') as csvfile:
    writer = csv.writer(csvfile)
    writer.writerow(["REVENUE"])  # write header
    writer.writerow(result)       # write the data row

# Close the cursor and the connection
cursor.close()
connection.close()
