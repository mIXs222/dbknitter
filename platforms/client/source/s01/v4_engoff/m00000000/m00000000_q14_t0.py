import pymysql
import csv
from datetime import datetime

# Connect to the MySQL database
connection = pymysql.connect(host='mysql',
                             user='root',
                             password='my-secret-pw',
                             database='tpch')

try:
    with connection.cursor() as cursor:
        # Define the SQL query
        query = """
        SELECT
            SUM(CASE WHEN p.p_type LIKE 'PROMO%' THEN l.l_extendedprice * (1 - l.l_discount) ELSE 0 END)
            / SUM(l.l_extendedprice * (1 - l.l_discount)) * 100 AS promo_revenue_percentage
        FROM
            part p
        JOIN
            lineitem l ON p.p_partkey = l.l_partkey
        WHERE
            l.l_shipdate >= '1995-09-01' AND l.l_shipdate < '1995-10-01';
        """

        # Execute the query
        cursor.execute(query)
        result = cursor.fetchone()

        # Write to CSV file
        with open('query_output.csv', 'w', newline='') as csvfile:
            csvwriter = csv.writer(csvfile)
            csvwriter.writerow(['promo_revenue_percentage'])
            csvwriter.writerow([result[0]])
finally:
    connection.close()
