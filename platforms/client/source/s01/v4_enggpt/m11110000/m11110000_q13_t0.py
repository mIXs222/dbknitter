import pymysql
import csv

# Connection details
mysql_connection_config = {
    'host': 'mysql',
    'user': 'root',
    'password': 'my-secret-pw',
    'database': 'tpch',
}

# Connect to MySQL server
mysql_conn = pymysql.connect(**mysql_connection_config)
mysql_cursor = mysql_conn.cursor()

# The adapted SQL query for MySQL
mysql_query = """
SELECT C_COUNT, COUNT(*) as 'CUSTDIST'
FROM (
    SELECT c.C_CUSTKEY, COUNT(o.O_ORDERKEY) as 'C_COUNT'
    FROM customer c
    LEFT JOIN orders o ON c.C_CUSTKEY = o.O_CUSTKEY
    AND o.O_COMMENT NOT LIKE '%pending%' AND o.O_COMMENT NOT LIKE '%deposits%'
    GROUP BY c.C_CUSTKEY
) as customer_orders
GROUP BY C_COUNT
ORDER BY CUSTDIST DESC, C_COUNT DESC;
"""

# Execute the query
mysql_cursor.execute(mysql_query)

# Write the query output to a CSV file
with open('query_output.csv', 'w', newline='') as csvfile:
    csv_writer = csv.writer(csvfile)
    # Write Headers
    csv_writer.writerow([i[0] for i in mysql_cursor.description])
    # Write Data
    csv_writer.writerows(mysql_cursor)

# Close cursor and connection
mysql_cursor.close()
mysql_conn.close()
