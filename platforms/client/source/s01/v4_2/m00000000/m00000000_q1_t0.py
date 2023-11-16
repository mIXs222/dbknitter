# filename: query.py
import pymysql
import csv

connection = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    db='tpch',
)

with connection.cursor() as cursor:
    cursor.execute("""
        SELECT
            L_RETURNFLAG,
            L_LINESTATUS,
            SUM(L_QUANTITY) AS SUM_QTY,
            SUM(L_EXTENDEDPRICE) AS SUM_BASE_PRICE,
            SUM(L_EXTENDEDPRICE * (1 - L_DISCOUNT)) AS SUM_DISC_PRICE,
            SUM(L_EXTENDEDPRICE * (1 - L_DISCOUNT) * (1 + L_TAX)) AS SUM_CHARGE,
            AVG(L_QUANTITY) AS AVG_QTY,
            AVG(L_EXTENDEDPRICE) AS AVG_PRICE,
            AVG(L_DISCOUNT) AS AVG_DISC,
            COUNT(*) AS COUNT_ORDER
        FROM
            lineitem
        WHERE
            L_SHIPDATE <= '1998-09-02'
        GROUP BY
            L_RETURNFLAG,
            L_LINESTATUS
        ORDER BY
            L_RETURNFLAG,
            L_LINESTATUS
    """)

    data = cursor.fetchall()

    headers = [i[0] for i in cursor.description]

    with open('query_output.csv', 'w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(headers)
        writer.writerows(data)

connection.close()
