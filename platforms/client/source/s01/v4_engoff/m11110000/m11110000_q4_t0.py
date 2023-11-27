import pymysql
import csv

# Connection information
mysql_conn_info = {
    'database': 'tpch',
    'user': 'root',
    'password': 'my-secret-pw',
    'host': 'mysql',
}

# Connect to MySQL database
mysql_conn = pymysql.connect(**mysql_conn_info)
mysql_cursor = mysql_conn.cursor()

# Query to execute
query = """
    SELECT
        o.O_ORDERPRIORITY,
        COUNT(DISTINCT o.O_ORDERKEY) AS order_count
    FROM
        orders AS o
    JOIN
        lineitem AS l
        ON o.O_ORDERKEY = l.L_ORDERKEY
    WHERE
        o.O_ORDERDATE >= '1993-07-01'
        AND o.O_ORDERDATE < '1993-10-01'
        AND l.L_RECEIPTDATE > l.L_COMMITDATE
    GROUP BY
        o.O_ORDERPRIORITY
    ORDER BY
        o.O_ORDERPRIORITY ASC;
"""

mysql_cursor.execute(query)
result = mysql_cursor.fetchall()

# Write result to CSV
with open('query_output.csv', 'w', newline='') as csvfile:
    csvwriter = csv.writer(csvfile)
    csvwriter.writerow(['O_ORDERPRIORITY', 'order_count'])
    for row in result:
        csvwriter.writerow(row)

# Close connections
mysql_cursor.close()
mysql_conn.close()
