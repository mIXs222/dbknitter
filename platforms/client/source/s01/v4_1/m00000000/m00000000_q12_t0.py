import csv
import pymysql.cursors
import pandas as pd

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

        # fetch all rows from the last executed SQL statement
        result = cursor.fetchall()
finally:
    connection.close()

# convert the result into pandas DataFrame
df = pd.DataFrame(result)

# write the DataFrame into csv
df.to_csv('query_output.csv', index=False)

