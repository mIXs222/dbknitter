# File: query_mysql.py

import pymysql
import csv

# Connect to the MySQL database
connection = pymysql.connect(host='mysql',
                             user='root',
                             password='my-secret-pw',
                             database='tpch')

try:
    with connection.cursor() as cursor:
        # Write the SQL query
        query = """
        SELECT SUM(L_EXTENDEDPRICE * L_DISCOUNT) AS REVENUE
        FROM lineitem
        WHERE L_SHIPDATE > '1994-01-01'
        AND L_SHIPDATE < '1995-01-01'
        AND L_DISCOUNT BETWEEN .05 AND .07
        AND L_QUANTITY < 24
        """
        # Execute the query
        cursor.execute(query)

        # Fetch the result
        result = cursor.fetchall()

        # Write the results to a CSV file
        with open('query_output.csv', 'w', newline='') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(['REVENUE'])  # write header
            for row in result:
                writer.writerow(row)

finally:
    connection.close()
