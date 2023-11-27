import csv
import pymysql.cursors

conn = pymysql.connect(host='mysql', user='root', password='my-secret-pw', db='tpch', charset='utf8mb4', cursorclass=pymysql.cursors.DictCursor)

try:
    with conn.cursor() as cursor:
        sql = """
            SELECT
                L_ORDERKEY,
                SUM(L_EXTENDEDPRICE * (1 - L_DISCOUNT)) AS REVENUE,
                O_ORDERDATE,
                O_SHIPPRIORITY
            FROM
                customer,
                orders,
                lineitem
            WHERE
                C_MKTSEGMENT = 'BUILDING'
                AND C_CUSTKEY = O_CUSTKEY
                AND L_ORDERKEY = O_ORDERKEY
                AND O_ORDERDATE < '1995-03-15'
                AND L_SHIPDATE > '1995-03-15'
            GROUP BY
                L_ORDERKEY,
                O_ORDERDATE,
                O_SHIPPRIORITY
            ORDER BY
                REVENUE DESC,
                O_ORDERDATE
            """
        cursor.execute(sql)
        result = cursor.fetchall()

        with open('query_output.csv', 'w', newline='') as file:
            writer = csv.writer(file)
            writer.writerows(result)

finally:
    conn.close()
