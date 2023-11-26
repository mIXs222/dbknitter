import pymysql
import csv

# MySQL connection parameters
db_params = {
    'database': 'tpch',
    'user': 'root',
    'password': 'my-secret-pw',
    'host': 'mysql',
}

# SQL query
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

# Establish the database connection
connection = pymysql.connect(**db_params)
try:
    with connection.cursor() as cursor:
        cursor.execute(query)
        result = cursor.fetchone()
        revenue = result[0] if result else 0

    # Write the output to a CSV file
    with open('query_output.csv', mode='w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(['REVENUE'])
        writer.writerow([revenue])

finally:
    connection.close()
