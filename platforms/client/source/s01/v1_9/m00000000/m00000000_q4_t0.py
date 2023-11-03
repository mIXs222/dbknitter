import csv
import pymysql.cursors

# Connect to the database
connection = pymysql.connect(host='mysql',
                             user='root',
                             password='my-secret-pw',
                             db='tpch',
                             charset='utf8mb4',
                             cursorclass=pymysql.cursors.DictCursor)

try:
    with connection.cursor() as cursor:
        # Execute the SQL query
        sql = """
        SELECT
            O_ORDERPRIORITY,
            COUNT(*) AS ORDER_COUNT
        FROM
            ORDERS
        WHERE
            O_ORDERDATE >= '1993-07-01'
            AND O_ORDERDATE < '1993-10-01'
            AND EXISTS (
                SELECT
                    *
                FROM
                    LINEITEM
                WHERE
                    L_ORDERKEY = O_ORDERKEY
                    AND L_COMMITDATE < L_RECEIPTDATE
                )
        GROUP BY
            O_ORDERPRIORITY
        ORDER BY
            O_ORDERPRIORITY
        """
        cursor.execute(sql)
        result = cursor.fetchall()

        # Write to csv
        with open('query_output.csv', 'w', newline='') as file:
            writer = csv.DictWriter(file, fieldnames=result[0].keys())
            writer.writeheader()
            writer.writerows(result)
finally:
    connection.close()
