import pymysql
import csv
from datetime import datetime

# Establish a connection to the mysql database
connection = pymysql.connect(host='mysql',
                             user='root',
                             password='my-secret-pw',
                             database='tpch',
                             cursorclass=pymysql.cursors.Cursor)

# Define the query to be executed
query = """
SELECT SUM(L_EXTENDEDPRICE * (1 - L_DISCOUNT)) AS total_revenue
FROM lineitem
WHERE L_SHIPDATE >= '1994-01-01' AND L_SHIPDATE <= '1994-12-31'
    AND L_DISCOUNT BETWEEN 0.05 AND 0.07
    AND L_QUANTITY < 24
"""

# Initialize cursor
cursor = connection.cursor()

try:
    # Execute the query
    cursor.execute(query)
    result = cursor.fetchone()

    # Write the output to a csv file
    with open('query_output.csv', 'w', newline='') as csvfile:
        csvwriter = csv.writer(csvfile)
        csvwriter.writerow(['total_revenue'])
        csvwriter.writerow(result)

finally:
    # Close the cursor and the connection
    cursor.close()
    connection.close()
