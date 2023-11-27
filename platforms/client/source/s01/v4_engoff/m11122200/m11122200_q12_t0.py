import pymysql
import csv
from datetime import datetime

# Define the query
query = """
SELECT
    l.L_SHIPMODE,
    CASE 
        WHEN o.O_ORDERPRIORITY = '1-URGENT'
            OR o.O_ORDERPRIORITY = '2-HIGH' THEN 'CRITICAL'
        ELSE 'NON-CRITICAL'
    END AS priority,
    COUNT(*) AS late_count
FROM
    lineitem l
JOIN
    orders o ON l.L_ORDERKEY = o.O_ORDERKEY
WHERE
    l.L_SHIPMODE IN ('MAIL', 'SHIP')
    AND l.L_RECEIPTDATE BETWEEN '1994-01-01' AND '1995-01-01'
    AND l.L_RECEIPTDATE > l.L_COMMITDATE
    AND l.L_SHIPDATE < l.L_COMMITDATE
GROUP BY
    l.L_SHIPMODE,
    priority;
"""

# MySQL connection parameters
mysql_params = {
    'host': 'mysql',
    'user': 'root',
    'password': 'my-secret-pw',
    'database': 'tpch'
}

def execute_query_and_write_output(query, connection_params):
    # Connect to the MySQL database
    connection = pymysql.connect(**connection_params)
    
    try:
        with connection.cursor() as cursor:
            cursor.execute(query)
            rows = cursor.fetchall()
            
            # Write the results to the file
            with open('query_output.csv', mode='w', newline='') as file:
                writer = csv.writer(file)
                writer.writerow(['SHIPMODE', 'PRIORITY', 'LATE_COUNT'])  # header
                for row in rows:
                    writer.writerow(row)
                    
    finally:
        connection.close()

# Execute the query and write output
if __name__ == '__main__':
    execute_query_and_write_output(query, mysql_params)
