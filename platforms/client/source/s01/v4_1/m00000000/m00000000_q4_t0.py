import pymysql
import csv

conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    db='tpch',
    charset='utf8',
)

try:
    with conn.cursor() as cursor:
        query = """
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
            O_ORDERPRIORITY;
        """
        cursor.execute(query)
        result = cursor.fetchall()

finally:
    conn.close()

# write output to csv
with open('query_output.csv', 'w', newline='') as f:
    writer = csv.writer(f)
    writer.writerow(['O_ORDERPRIORITY', 'ORDER_COUNT'])
    for row in result:
        writer.writerow(list(row))
