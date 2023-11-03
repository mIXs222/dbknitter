import mysql.connector
import csv

# Creating the connection
mydb = mysql.connector.connect(
        host="mysql",
        user="root",
        password="my-secret-pw",
        database="tpch"
)

mycursor = mydb.cursor()

# SQL Query
query = """
SELECT
    L_SHIPMODE,
    SUM(CASE WHEN O_ORDERPRIORITY = '1-URGENT'
             OR O_ORDERPRIORITY = '2-HIGH' THEN 1 ELSE 0 END) AS HIGH_LINE_COUNT,
    SUM(CASE WHEN O_ORDERPRIORITY <> '1-URGENT'
             AND O_ORDERPRIORITY <> '2-HIGH' THEN 1 ELSE 0 END) AS LOW_LINE_COUNT
FROM
    orders, lineitem
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

# Execute the query
mycursor.execute(query)

# Write the results to a CSV file
with open('query_output.csv', 'w', newline='') as f:
    writer = csv.writer(f)
    writer.writerow([i[0] for i in mycursor.description])  # write headers
    writer.writerows(mycursor.fetchall())  # write data
