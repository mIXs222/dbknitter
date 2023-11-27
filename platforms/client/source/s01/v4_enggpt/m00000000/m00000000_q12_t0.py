import pymysql
import csv

# MySQL connection setup
mysql_con_config = {
    'user': 'root',
    'password': 'my-secret-pw',
    'host': 'mysql',
    'database': 'tpch',
}

# Connect to MySQL database
mysql_con = pymysql.connect(**mysql_con_config)
mysql_cursor = mysql_con.cursor()

# The SQL query to be executed
mysql_query = """
SELECT L_SHIPMODE,
    SUM(CASE WHEN O_ORDERPRIORITY IN ('1-URGENT', '2-HIGH') THEN 1 ELSE 0 END) AS HIGH_LINE_COUNT,
    SUM(CASE WHEN O_ORDERPRIORITY NOT IN ('1-URGENT', '2-HIGH') THEN 1 ELSE 0 END) AS LOW_LINE_COUNT
FROM lineitem
JOIN orders ON L_ORDERKEY = O_ORDERKEY
WHERE L_SHIPMODE IN ('MAIL', 'SHIP')
AND L_COMMITDATE < L_RECEIPTDATE
AND L_SHIPDATE < L_COMMITDATE
AND L_RECEIPTDATE BETWEEN '1994-01-01' AND '1994-12-31'
GROUP BY L_SHIPMODE
ORDER BY L_SHIPMODE ASC
"""

# Execute the MySQL query
mysql_cursor.execute(mysql_query)

# Fetch all rows from the query result
data = mysql_cursor.fetchall()

# Write the query result into a CSV file
with open('query_output.csv', 'w', newline='') as csvfile:
    output_writer = csv.writer(csvfile)
    # Write header
    output_writer.writerow(['L_SHIPMODE', 'HIGH_LINE_COUNT', 'LOW_LINE_COUNT'])
    # Write data rows
    for row in data:
        output_writer.writerow(row)

# Close cursor and connection
mysql_cursor.close()
mysql_con.close()
