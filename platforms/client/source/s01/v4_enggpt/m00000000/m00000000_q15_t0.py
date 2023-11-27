import pymysql
import csv
from datetime import datetime

# Connect to MySQL
connection = pymysql.connect(host='mysql',
                             user='root',
                             password='my-secret-pw',
                             db='tpch')

try:
    with connection.cursor() as cursor:
        # Create the revenue0 Common Table Expression
        cte_query = """
            WITH revenue0 AS (
                SELECT L_SUPPKEY AS SUPPLIER_NO,
                SUM(L_EXTENDEDPRICE * (1 - L_DISCOUNT)) AS TOTAL_REVENUE
                FROM lineitem
                WHERE L_SHIPDATE BETWEEN '1996-01-01' AND '1996-03-31'
                GROUP BY L_SUPPKEY
            )
        """
        # Determining the maximum total revenue from the CTE
        max_rev_query = """
            SELECT MAX(TOTAL_REVENUE) FROM revenue0
        """
        cursor.execute(cte_query + max_rev_query)
        max_revenue = cursor.fetchone()[0]

        # Get all supplier details and their total revenue
        final_query = """
        SELECT S.S_SUPPKEY, S.S_NAME, S.S_ADDRESS, S.S_PHONE, R0.TOTAL_REVENUE
        FROM supplier AS S
        JOIN revenue0 R0 ON S.S_SUPPKEY = R0.SUPPLIER_NO
        WHERE R0.TOTAL_REVENUE = %s
        ORDER BY S.S_SUPPKEY ASC
        """
        cursor.execute(cte_query + final_query, (max_revenue,))
        results = cursor.fetchall()

        # Writing output to a CSV file
        with open('query_output.csv', mode='w', newline='') as file:
            writer = csv.writer(file)
            writer.writerow(['S_SUPPKEY', 'S_NAME', 'S_ADDRESS', 'S_PHONE', 'TOTAL_REVENUE'])
            for row in results:
                writer.writerow(row)

finally:
    connection.close()
