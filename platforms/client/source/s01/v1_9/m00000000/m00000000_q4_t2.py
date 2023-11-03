import pymysql
import csv
import pandas as pd

# Establish MySQL connection
connection = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    db='tpch'
)

try:

    with connection.cursor() as cursor:
        sql_query = """
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
        """

        cursor.execute(sql_query)
        rows = cursor.fetchall()

        fields = ['O_ORDERPRIORITY', 'ORDER_COUNT']
        rows = [list(item.values()) for item in rows]

        filename = "query_output.csv"

        with open(filename, 'w') as csvfile:
            csvwriter = csv.writer(csvfile)
            csvwriter.writerow(fields)
            csvwriter.writerows(rows)
            
        print(f"Results exported to {filename} successfully.")

finally:
    connection.close()

