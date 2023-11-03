import mysql.connector
import csv

#establishing the connection
conn = mysql.connector.connect(user='root', password='my-secret-pw', host='mysql', database='tpch')
#Creating a cursor object using the cursor() method
cursor = conn.cursor()

#Executing an MYSQL function using the execute() method
cursor.execute("""
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
""")

result = cursor.fetchall()

#Writing to csv file
with open('query_output.csv', 'w', newline='') as file:
	writer = csv.writer(file)
	writer.writerows(result)

#close the cursor and connection
cursor.close()
conn.close()
