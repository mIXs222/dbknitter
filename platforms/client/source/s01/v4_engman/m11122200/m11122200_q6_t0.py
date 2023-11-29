import pymysql
import csv

# Connection details
connection_params = {
    'host': 'mysql',
    'user': 'root',
    'password': 'my-secret-pw',
    'db': 'tpch',
}

# Query to execute
query = """
SELECT SUM(L_EXTENDEDPRICE * L_DISCOUNT) as REVENUE
FROM lineitem
WHERE L_SHIPDATE > '1994-01-01' AND L_SHIPDATE < '1995-01-01'
AND L_DISCOUNT BETWEEN 0.06 - 0.01 AND 0.06 + 0.01
AND L_QUANTITY < 24;
"""

# Set up the database connection
connection = pymysql.connect(**connection_params)

try:
    with connection.cursor() as cursor:
        cursor.execute(query)
        result = cursor.fetchone()
        with open('query_output.csv', 'w', newline='') as csvfile:
            csv_writer = csv.writer(csvfile)
            csv_writer.writerow(['REVENUE'])
            csv_writer.writerow(result)
finally:
    connection.close()
