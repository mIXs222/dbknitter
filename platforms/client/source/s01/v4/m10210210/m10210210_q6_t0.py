# file: query_code.py
import pymysql
import csv

# Connection details
connection_details = {
    'host': 'mysql',
    'user': 'root',
    'password': 'my-secret-pw',
    'db': 'tpch',
    'charset': 'utf8mb4'
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

# Execute the query and write the results to a CSV file
with pymysql.connect(**connection_details) as connection:
    with connection.cursor() as cursor:
        cursor.execute(query)
        result = cursor.fetchone()
        with open('query_output.csv', 'w', newline='') as csvfile:
            csvwriter = csv.writer(csvfile)
            csvwriter.writerow(['REVENUE'])
            csvwriter.writerow(result)
