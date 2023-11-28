import pymysql
import csv

# Database connection
connection = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    database='tpch'
)

# SQL query to perform the analysis
query = """
SELECT L_SHIPMODE, 
    SUM(CASE WHEN O_ORDERPRIORITY IN ('1-URGENT', '2-HIGH') THEN 1 ELSE 0 END) AS HIGH_LINE_COUNT,
    SUM(CASE WHEN O_ORDERPRIORITY NOT IN ('1-URGENT', '2-HIGH') THEN 1 ELSE 0 END) AS LOW_LINE_COUNT
FROM orders
INNER JOIN lineitem ON O_ORDERKEY = L_ORDERKEY
WHERE L_SHIPMODE IN ('MAIL', 'SHIP')
AND L_COMMITDATE < L_RECEIPTDATE
AND L_SHIPDATE < L_COMMITDATE
AND L_RECEIPTDATE BETWEEN '1994-01-01' AND '1994-12-31'
GROUP BY L_SHIPMODE
ORDER BY L_SHIPMODE;
"""

# Execute the query and fetch the results
with connection.cursor() as cursor:
    cursor.execute(query)
    query_results = cursor.fetchall()

# Write query results to a CSV file
with open('query_output.csv', mode='w', newline='') as csv_file:
    csv_writer = csv.writer(csv_file)
    # Writing the headers
    csv_writer.writerow(['L_SHIPMODE', 'HIGH_LINE_COUNT', 'LOW_LINE_COUNT'])
    # Writing the data
    for row in query_results:
        csv_writer.writerow(row)

# Close the database connection
connection.close()
