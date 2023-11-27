# query.py
import pymysql
import csv

# MySQL connection parameters
db_params = {
    'host': 'mysql',
    'user': 'root',
    'password': 'my-secret-pw',
    'db': 'tpch',
}

# SQL Query
sql_query = """
SELECT SUM(L_EXTENDEDPRICE * L_DISCOUNT) AS potential_revenue_increase
FROM lineitem
WHERE L_SHIPDATE BETWEEN '1994-01-01' AND '1995-01-01'
AND L_DISCOUNT BETWEEN 0.06 - 0.01 AND 0.06 + 0.01
AND L_QUANTITY < 24;
"""

# Connect to MySQL database
connection = pymysql.connect(**db_params)
try:
    with connection.cursor() as cursor:
        cursor.execute(sql_query)
        result = cursor.fetchone()
        # Write the result to a CSV file
        with open('query_output.csv', 'w', newline='') as csvfile:
            csvwriter = csv.writer(csvfile)
            csvwriter.writerow(['potential_revenue_increase'])
            csvwriter.writerow(result)
finally:
    connection.close()
