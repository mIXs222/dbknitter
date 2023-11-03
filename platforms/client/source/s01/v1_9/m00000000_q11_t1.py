import csv
import mysql.connector

# Establish a connection to the MySQL database
db = mysql.connector.connect(
  host="mysql",
  user="root",
  password="my-secret-pw",
  database="tpch"
)

# Get a cursor object
cursor = db.cursor()

# Define the query
query = """
SELECT PS_PARTKEY, SUM(PS_SUPPLYCOST * PS_AVAILQTY) AS VALUE
FROM partsupp, supplier, nation
WHERE PS_SUPPKEY = S_SUPPKEY AND S_NATIONKEY = N_NATIONKEY AND N_NAME = 'GERMANY'
GROUP BY PS_PARTKEY HAVING SUM(PS_SUPPLYCOST * PS_AVAILQTY) > 
(SELECT SUM(PS_SUPPLYCOST * PS_AVAILQTY) * 0.0001000000 FROM partsupp, supplier, nation 
WHERE PS_SUPPKEY = S_SUPPKEY AND S_NATIONKEY = N_NATIONKEY AND N_NAME = 'GERMANY')
ORDER BY VALUE DESC
"""

# Execute the query
cursor.execute(query)

# Fetch all the records
rows = cursor.fetchall()

# Write the output to a CSV file
with open('query_output.csv', 'w', newline='') as f:
    writer = csv.writer(f)
    writer.writerow(['PS_PARTKEY', 'VALUE'])  # writing headers
    writer.writerows(rows) # writing data

# Close the connection
db.close()
