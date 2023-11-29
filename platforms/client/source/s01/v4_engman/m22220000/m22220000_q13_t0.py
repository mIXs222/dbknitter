import pymysql
import csv

# Connection parameters
mysql_db_config = {
    'host': 'mysql',
    'user': 'root',
    'password': 'my-secret-pw',
    'database': 'tpch'
}

# Connect to MySQL database
mysql_conn = pymysql.connect(**mysql_db_config)
mysql_cursor = mysql_conn.cursor()

# SQL Query
sql_query = """
SELECT num_orders, COUNT(*) as num_customers FROM (
    SELECT O_CUSTKEY, COUNT(*) AS num_orders
    FROM orders
    WHERE O_COMMENT NOT LIKE '%pending%deposits%'
    GROUP BY O_CUSTKEY
) AS order_counts
GROUP BY num_orders
ORDER BY num_orders;
"""

# Execute the query on MySQL and fetch the result
mysql_cursor.execute(sql_query)
query_result = mysql_cursor.fetchall()

# Write to CSV file
with open('query_output.csv', 'w', newline='') as csvfile:
    csvwriter = csv.writer(csvfile)
    csvwriter.writerow(['num_orders', 'num_customers'])
    for row in query_result:
        csvwriter.writerow(row)

# Close connections
mysql_cursor.close()
mysql_conn.close()
