import csv
import mysql.connector

cnx = mysql.connector.connect(user='root', password='my-secret-pw', host='mysql', database='tpch')
cursor = cnx.cursor()

query = """
    SELECT
        L_SHIPMODE,
        SUM(CASE
                WHEN O_ORDERPRIORITY = '1-URGENT'
                OR O_ORDERPRIORITY = '2-HIGH'
                THEN 1
                ELSE 0
        END) AS HIGH_LINE_COUNT,
        SUM(CASE
                WHEN O_ORDERPRIORITY <> '1-URGENT'
                AND O_ORDERPRIORITY <> '2-HIGH'
                THEN 1
                ELSE 0
        END) AS LOW_LINE_COUNT
    FROM
        orders,
        lineitem
    WHERE
        O_ORDERKEY = L_ORDERKEY
        AND L_SHIPMODE IN ('MAIL', 'SHIP')
        AND L_COMMITDATE < L_RECEIPTDATE
        AND L_SHIPDATE < L_COMMITDATE
        AND L_RECEIPTDATE >= '1994-01-01'
        AND L_RECEIPTDATE < '1995-01-01'
    GROUP BY
        L_SHIPMODE
    ORDER BY
        L_SHIPMODE
"""

cursor.execute(query)

with open('query_output.csv', 'w', newline='') as file:
    writer = csv.writer(file)
    writer.writerow([i[0] for i in cursor.description])  # write headers
    writer.writerows(cursor.fetchall())

cursor.close()
cnx.close()
