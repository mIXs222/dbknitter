# orders_and_lineitems.py
import pymysql
import csv

# Connect to the MySQL database
connection = pymysql.connect(host='mysql',
                             user='root',
                             password='my-secret-pw',
                             database='tpch')

try:
    with connection.cursor() as cursor:
        # SQL query to count orders with late lineitems
        query = """
            SELECT
                O_ORDERPRIORITY,
                COUNT(*) AS order_count
            FROM
                orders
            JOIN
                lineitem ON O_ORDERKEY = L_ORDERKEY
            WHERE
                O_ORDERDATE BETWEEN '1993-07-01' AND '1993-10-01'
                AND L_COMMITDATE < L_RECEIPTDATE
            GROUP BY
                O_ORDERPRIORITY
            ORDER BY
                O_ORDERPRIORITY;
        """
        cursor.execute(query)
        result = cursor.fetchall()

    # Write query output to CSV file
    with open('query_output.csv', 'w', newline='') as csvfile:
        csvwriter = csv.writer(csvfile)
        # Write header
        csvwriter.writerow(['O_ORDERPRIORITY', 'order_count'])
        # Write data
        for row in result:
            csvwriter.writerow([row[0], row[1]])

finally:
    connection.close()
