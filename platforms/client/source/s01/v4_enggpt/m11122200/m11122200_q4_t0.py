import pymysql
import csv
from datetime import datetime

# MySQL connection configuration
mysql_config = {
    'host': 'mysql',
    'user': 'root',
    'password': 'my-secret-pw',
    'db': 'tpch',
}

# Connect to MySQL
mysql_conn = pymysql.connect(**mysql_config)
mysql_cursor = mysql_conn.cursor()

# Query to retrieve the necessary data from MySQL
mysql_query = """
SELECT 
    o.O_ORDERPRIORITY,
    COUNT(DISTINCT o.O_ORDERKEY) AS order_count
FROM 
    orders o
JOIN 
    lineitem l 
ON 
    o.O_ORDERKEY = l.L_ORDERKEY
WHERE 
    o.O_ORDERDATE BETWEEN '1993-07-01' AND '1993-10-01'
AND 
    l.L_COMMITDATE < l.L_RECEIPTDATE
GROUP BY 
    o.O_ORDERPRIORITY
ORDER BY 
    o.O_ORDERPRIORITY ASC;
"""

# Execute the query
mysql_cursor.execute(mysql_query)

# Fetch the results
results = mysql_cursor.fetchall()

# Close the MySQL connection
mysql_cursor.close()
mysql_conn.close()

# Write results to CSV
with open('query_output.csv', 'w', newline='') as csvfile:
    fieldnames = ['O_ORDERPRIORITY', 'order_count']
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
    
    writer.writeheader()
    for row in results:
        writer.writerow({'O_ORDERPRIORITY': row[0], 'order_count': row[1]})
