import pymysql
import csv

# Define the query
query = """
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

# Connect to the database
connection = pymysql.connect(host='mysql',
                             user='root',
                             password='my-secret-pw',
                             database='tpch')

try:
    # Execute the query and fetch the results
    with connection.cursor() as cursor:
        cursor.execute(query)
        result = cursor.fetchone()
        revenue = result[0] if result else None
    
    # Write result to a CSV file
    with open('query_output.csv', 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(['REVENUE'])
        writer.writerow([revenue])

finally:
    # Close the connection
    connection.close()
