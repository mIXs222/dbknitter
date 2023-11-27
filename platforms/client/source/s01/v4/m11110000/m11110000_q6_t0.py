# query_script.py

import pymysql
import csv

# MySQL connection details
mysql_connection_details = {
    'host': 'mysql',
    'user': 'root',
    'password': 'my-secret-pw',
    'database': 'tpch'
}

# SQL query
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

# Execute the query and write results to CSV file
def execute_query_and_write_output(connection_details, query):
    # Connect to the MySQL database
    connection = pymysql.connect(
        host=connection_details['host'],
        user=connection_details['user'],
        password=connection_details['password'],
        database=connection_details['database'],
    )

    try:
        with connection.cursor() as cursor:
            cursor.execute(query)
            result = cursor.fetchone()
            with open('query_output.csv', mode='w') as file:
                writer = csv.writer(file)
                writer.writerow(['REVENUE'])
                writer.writerow(result)
    finally:
        connection.close()

# Run the functions
if __name__ == '__main__':
    execute_query_and_write_output(mysql_connection_details, sql_query)
