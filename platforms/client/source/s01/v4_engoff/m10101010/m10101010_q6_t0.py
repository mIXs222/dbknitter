import pymysql
import csv

# Connection information
connection_params = {
    "host": "mysql",
    "user": "root",
    "password": "my-secret-pw",
    "database": "tpch"
}

# Connect to MySQL database
connection = pymysql.connect(**connection_params)
cursor = connection.cursor()

# SQL query
query = """
SELECT sum(L_EXTENDEDPRICE * L_DISCOUNT) as revenue_change
FROM lineitem
WHERE 
    L_SHIPDATE BETWEEN '1994-01-01' AND '1995-01-01'
    AND L_DISCOUNT BETWEEN 0.06 - 0.01 AND 0.06 + 0.01
    AND L_QUANTITY < 24
"""

# Execute query
cursor.execute(query)

# Write query results to CSV
with open('query_output.csv', mode='w', newline='') as file:
    writer = csv.writer(file)
    
    # Write header
    writer.writerow(['revenue_change'])
    
    # Write data
    for row in cursor:
        writer.writerow(row)

# Close cursor and connection
cursor.close()
connection.close()
