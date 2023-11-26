import pymysql
import csv

# MySQL connection details
mysql_conn_info = {
    'host': 'mysql',
    'user': 'root',
    'password': 'my-secret-pw',
    'database': 'tpch'
}

# Connect to MySQL database
mysql_conn = pymysql.connect(**mysql_conn_info)
mysql_cursor = mysql_conn.cursor()

# MySQL query
mysql_query = """
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

# Execute the query on mysql database
mysql_cursor.execute(mysql_query)

# Write the results to a CSV file
with open('query_output.csv', 'w', newline='') as csvfile:
    csvwriter = csv.writer(csvfile)
    # Write header
    csvwriter.writerow(['L_SHIPMODE', 'HIGH_LINE_COUNT', 'LOW_LINE_COUNT'])
    # Write data rows
    for row in mysql_cursor:
        csvwriter.writerow(row)

# Close the cursor and connection
mysql_cursor.close()
mysql_conn.close()
