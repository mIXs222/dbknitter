# query.py

import pymysql
import csv

# Establish connection to MySQL database using pymysql
connection = pymysql.connect(
    host="mysql",
    user="root",
    password="my-secret-pw",
    database="tpch",
    charset='utf8mb4',
    cursorclass=pymysql.cursors.Cursor
)

try:
    with connection.cursor() as cursor:
        # Write SQL query that computes total revenue
        sql_query = """
        SELECT SUM(L_EXTENDEDPRICE * (1 - L_DISCOUNT)) as total_revenue
        FROM lineitem
        WHERE L_SHIPDATE >= '1994-01-01'
          AND L_SHIPDATE <= '1994-12-31'
          AND L_DISCOUNT BETWEEN 0.05 AND 0.07
          AND L_QUANTITY < 24;
        """

        cursor.execute(sql_query)

        # Fetch result from query
        total_revenue = cursor.fetchone()

        # Write output to CSV file
        with open('query_output.csv', 'w', newline='') as csvfile:
            csvwriter = csv.writer(csvfile)
            csvwriter.writerow(['total_revenue'])
            csvwriter.writerow(total_revenue)

finally:
    connection.close()
