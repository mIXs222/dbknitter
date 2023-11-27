# query.py

import pymysql
import csv

# MySQL connection parameters
db_params = {
    'host': 'mysql',
    'user': 'root',
    'password': 'my-secret-pw',
    'db': 'tpch'
}

# SQL query
sql_query = '''
SELECT
    SUM(L_EXTENDEDPRICE * L_DISCOUNT) AS REVENUE
FROM
    lineitem
WHERE
    L_SHIPDATE >= '1994-01-01'
    AND L_SHIPDATE < '1995-01-01'
    AND L_DISCOUNT BETWEEN .06 - 0.01 AND .06 + 0.01
    AND L_QUANTITY < 24
'''

# Connect to the MySQL database
conn = pymysql.connect(**db_params)
try:
    with conn.cursor() as cursor:
        cursor.execute(sql_query)
        result = cursor.fetchone()

        # Write the result to a CSV file
        with open('query_output.csv', 'w', newline='') as csvfile:
            csvwriter = csv.writer(csvfile)
            csvwriter.writerow(['REVENUE'])
            csvwriter.writerow(result)
finally:
    conn.close()
