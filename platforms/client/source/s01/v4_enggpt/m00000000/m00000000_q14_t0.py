import pymysql
import csv
from datetime import datetime

# Establish a connection to the MySQL database
connection = pymysql.connect(host='mysql',
                             user='root',
                             password='my-secret-pw',
                             database='tpch')

try:
    with connection.cursor() as cursor:
        # Query for promotional revenue
        promo_revenue_query = """
        SELECT SUM(L_EXTENDEDPRICE * (1 - L_DISCOUNT)) AS promo_revenue
        FROM lineitem
        JOIN part ON lineitem.L_PARTKEY = part.P_PARTKEY
        WHERE part.P_TYPE LIKE 'PROMO%%'
        AND lineitem.L_SHIPDATE BETWEEN '1995-09-01' AND '1995-09-30';
        """
        cursor.execute(promo_revenue_query)
        promo_revenue = cursor.fetchone()[0]

        # Query for total revenue
        total_revenue_query = """
        SELECT SUM(L_EXTENDEDPRICE * (1 - L_DISCOUNT)) AS total_revenue
        FROM lineitem
        WHERE L_SHIPDATE BETWEEN '1995-09-01' AND '1995-09-30';
        """
        cursor.execute(total_revenue_query)
        total_revenue = cursor.fetchone()[0]

        # Calculate promotional revenue percentage
        promo_revenue_percentage = (promo_revenue / total_revenue) * 100 if total_revenue else 0

    # Write the results to a CSV file
    with open('query_output.csv', 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(['Promo Revenue', 'Total Revenue', 'Promo Revenue Percentage'])
        writer.writerow([promo_revenue, total_revenue, promo_revenue_percentage])
finally:
    connection.close()
