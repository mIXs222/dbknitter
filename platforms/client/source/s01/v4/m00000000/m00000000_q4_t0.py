import pymysql
import csv

# Establish connection to the MySQL database
connection = pymysql.connect(host='mysql',
                             user='root',
                             password='my-secret-pw',
                             database='tpch',
                             charset='utf8mb4',
                             cursorclass=pymysql.cursors.Cursor)

try:
    with connection.cursor() as cursor:
        # Execute the SQL query
        cursor.execute("""
            SELECT
                O_ORDERPRIORITY,
                COUNT(*) AS ORDER_COUNT
            FROM
                orders
            WHERE
                O_ORDERDATE >= '1993-07-01'
                AND O_ORDERDATE < '1993-10-01'
                AND EXISTS (
                    SELECT
                        *
                    FROM
                        lineitem
                    WHERE
                        L_ORDERKEY = O_ORDERKEY
                        AND L_COMMITDATE < L_RECEIPTDATE
                )
            GROUP BY
                O_ORDERPRIORITY
            ORDER BY
                O_ORDERPRIORITY
        """)

        # Fetch all the results
        results = cursor.fetchall()

        # Write the results to a CSV file
        with open('query_output.csv', 'w', newline='') as csvfile:
            csv_writer = csv.writer(csvfile)
            # Write header
            csv_writer.writerow(['O_ORDERPRIORITY', 'ORDER_COUNT'])
            # Write data rows
            for row in results:
                csv_writer.writerow(row)
finally:
    connection.close()
