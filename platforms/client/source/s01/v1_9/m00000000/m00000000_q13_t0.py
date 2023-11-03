import mysql.connector
import csv

# Setup mysql connection
db = mysql.connector.connect(
  host="mysql",
  user="root",
  password="my-secret-pw",
  database="tpch"
)

# Create a cursor
cursor = db.cursor()

# SQL query string
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

# Executing query
cursor.execute(sql_query)

# Fetching all rows
rows = cursor.fetchall()

# Column names
column_names = [i[0] for i in cursor.description]

# Writing to csv
with open('query_output.csv', 'w', newline='') as f:
    writer = csv.writer(f)
    writer.writerow(column_names)
    writer.writerows(rows)

# Close db connection
db.close()
