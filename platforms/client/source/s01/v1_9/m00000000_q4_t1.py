import mysql.connector
import csv

connection = mysql.connector.connect(
    host="mysql",
    user="root",
    password="my-secret-pw",
    database="tpch"
)

cursor = connection.cursor()

cursor.execute("""
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
""")

rows = cursor.fetchall()
with open('query_output.csv', 'w', newline='') as csvfile:
    fieldnames = ['O_ORDERPRIORITY', 'ORDER_COUNT']
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
    writer.writeheader()
    for row in rows:
        writer.writerow({'O_ORDERPRIORITY': row[0], 'ORDER_COUNT': row[1]})
