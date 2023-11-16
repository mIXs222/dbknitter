import pymysql
import csv

# Connection information
mysql_connection_info = {
    'host': 'mysql',
    'user': 'root',
    'password': 'my-secret-pw',
    'db': 'tpch',
}

# Connect to MySQL
mysql_conn = pymysql.connect(**mysql_connection_info)
cursor = mysql_conn.cursor()

# Execute the query
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

# Write the output to a csv file
with open('query_output.csv', 'w', newline='') as csvfile:
    csvwriter = csv.writer(csvfile)
    # Write the header
    csvwriter.writerow([i[0] for i in cursor.description])
    # Write the data
    csvwriter.writerows(cursor.fetchall())

# Close the MySQL connection
cursor.close()
mysql_conn.close()
