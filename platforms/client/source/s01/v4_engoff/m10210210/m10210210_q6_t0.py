import pymysql
import csv

# Connection information
connection_params = {
    'host': 'mysql',
    'user': 'root',
    'password': 'my-secret-pw',
    'database': 'tpch',
}

# SQL query
sql_query = """
SELECT SUM(L_EXTENDEDPRICE * L_DISCOUNT) AS potential_revenue_increase
FROM lineitem
WHERE L_SHIPDATE >= '1994-01-01'
  AND L_SHIPDATE < '1995-01-01'
  AND L_DISCOUNT BETWEEN 0.06 - 0.01 AND 0.06 + 0.01
  AND L_QUANTITY < 24
"""

# Establish a MySQL connection
connection = pymysql.connect(**connection_params)
try:
    with connection.cursor() as cursor:
        cursor.execute(sql_query)
        result = cursor.fetchone()

        # Write to CSV file
        with open('query_output.csv', mode='w', newline='') as file:
            writer = csv.writer(file)
            writer.writerow(['potential_revenue_increase'])
            writer.writerow([result[0]])
finally:
    # Close the connection
    connection.close()
