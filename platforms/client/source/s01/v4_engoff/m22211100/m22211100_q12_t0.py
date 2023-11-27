# query.py
import pymysql
import csv

# Connection information
mysql_connection_info = {
    'host': 'mysql',
    'user': 'root',
    'password': 'my-secret-pw',
    'db': 'tpch',
    'charset': 'utf8mb4',
    'cursorclass': pymysql.cursors.Cursor
}

# Connect to MySQL
mysql_conn = pymysql.connect(**mysql_connection_info)

try:
    with mysql_conn.cursor() as cursor:
        # MySQL Query
        mysql_query = """
        SELECT
            L_SHIPMODE,
            O_ORDERPRIORITY,
            COUNT(*) AS late_lineitems_count
        FROM
            lineitem
        JOIN orders ON
            L_ORDERKEY = O_ORDERKEY
        WHERE
            L_SHIPMODE IN ('MAIL', 'SHIP')
            AND L_RECEIPTDATE BETWEEN '1994-01-01' AND '1995-01-01'
            AND L_RECEIPTDATE > L_COMMITDATE
            AND L_SHIPDATE < L_COMMITDATE 
            AND (O_ORDERPRIORITY = 'URGENT' OR O_ORDERPRIORITY = 'HIGH')
        GROUP BY
            L_SHIPMODE,
            O_ORDERPRIORITY
        """
        cursor.execute(mysql_query)
        result = cursor.fetchall()

        # Writing to query_output.csv
        with open('query_output.csv', 'w', newline='') as csvfile:
            csv_writer = csv.writer(csvfile)
            csv_writer.writerow(['L_SHIPMODE', 'O_ORDERPRIORITY', 'late_lineitems_count'])
            for row in result:
                csv_writer.writerow(row)

finally:
    mysql_conn.close()
