import pymysql
import csv

# Define MySQL connection parameters
mysql_params = {
    'host': 'mysql',
    'user': 'root',
    'password': 'my-secret-pw',
    'db': 'tpch',
    'charset': 'utf8mb4',
    'cursorclass': pymysql.cursors.Cursor
}

# Establish a connection to MySQL
mysql_conn = pymysql.connect(**mysql_params)
try:
    with mysql_conn.cursor() as cursor:
        # SQL Query
        sql = """
        SELECT o.O_ORDERPRIORITY, COUNT(DISTINCT o.O_ORDERKEY) AS order_count
        FROM orders o
        INNER JOIN lineitem l ON o.O_ORDERKEY = l.L_ORDERKEY
        WHERE o.O_ORDERDATE >= '1993-07-01'
        AND o.O_ORDERDATE < '1993-10-01'
        AND l.L_COMMITDATE < l.L_RECEIPTDATE
        GROUP BY o.O_ORDERPRIORITY
        ORDER BY o.O_ORDERPRIORITY ASC;
        """

        cursor.execute(sql)
        results = cursor.fetchall()

        # Write the results to a CSV file
        with open('query_output.csv', 'w', newline='') as csvfile:
            csvwriter = csv.writer(csvfile)
            csvwriter.writerow(['O_ORDERPRIORITY', 'order_count'])  # Header
            for row in results:
                csvwriter.writerow(row)
finally:
    mysql_conn.close()
