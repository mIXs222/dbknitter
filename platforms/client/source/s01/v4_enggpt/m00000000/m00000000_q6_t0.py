# query.py

import pymysql
import csv
from datetime import datetime

# Establish a connection to the MySQL database
connection = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    db='tpch',
    charset='utf8mb4'
)

# Prepare a SQL query string.
# The WHERE clause filters the rows to those with a SHIPDATE between 1994-01-01 and 1994-12-31,
# with a DISCOUNT between 0.05 and 0.07, and a QUANTITY less than 24.
sql_query = """
SELECT SUM(L_EXTENDEDPRICE * (1 - L_DISCOUNT)) as total_revenue
FROM lineitem
WHERE L_SHIPDATE >= '1994-01-01' 
  AND L_SHIPDATE <= '1994-12-31' 
  AND L_DISCOUNT BETWEEN 0.05 AND 0.07 
  AND L_QUANTITY < 24;
"""

try:
    with connection.cursor() as cursor:
        # Execute the SQL query
        cursor.execute(sql_query)
        
        # Fetch the result
        result = cursor.fetchone()
        total_revenue = result[0]
        
        # Write the result to query_output.csv
        with open('query_output.csv', 'w', newline='') as csvfile:
            writer = csv.writer(csvfile)
            # Write the header
            writer.writerow(['total_revenue'])
            # Write the data
            writer.writerow([total_revenue])

finally:
    # Close the connection
    connection.close()
