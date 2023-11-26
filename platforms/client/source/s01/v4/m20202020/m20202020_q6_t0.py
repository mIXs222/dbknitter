# query.py
import csv
import pymysql

# Connection information
conn_info = {
    'host': 'mysql',
    'user': 'root',
    'password': 'my-secret-pw',
    'db': 'tpch',
    'charset': 'utf8mb4'
}

# SQL Query
sql_query = """
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
try:
    connection = pymysql.connect(**conn_info)
    with connection:
        with connection.cursor() as cursor:
            cursor.execute(sql_query)
            result = cursor.fetchone()
            with open('query_output.csv', 'w', newline='') as file:
                writer = csv.writer(file)
                writer.writerow(['REVENUE'])
                writer.writerow(result)
except Exception as e:
    print(f"An error occurred: {e}")
finally:
    connection.close()
