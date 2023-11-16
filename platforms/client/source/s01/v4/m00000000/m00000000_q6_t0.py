# query_database.py
import pymysql
import csv

# MySQL connection parameters
mysql_params = {
    'host': 'mysql',
    'user': 'root',
    'password': 'my-secret-pw',
    'database': 'tpch'
}

# Connect to MySQL database
mysql_connection = pymysql.connect(**mysql_params)
mysql_cursor = mysql_connection.cursor()

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

try:
    # Execute query in MySQL database
    mysql_cursor.execute(query)

    # Write output to CSV file
    with open('query_output.csv', 'w', newline='') as csvfile:
        csvwriter = csv.writer(csvfile)
        csvwriter.writerow(['REVENUE'])  # Write header
        for row in mysql_cursor:
            csvwriter.writerow(row)

finally:
    mysql_cursor.close()
    mysql_connection.close()

