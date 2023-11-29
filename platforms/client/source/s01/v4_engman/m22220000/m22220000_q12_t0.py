import pymysql
import csv

# Define connection parameters
MYSQL_DB = 'tpch'
MYSQL_USER = 'root'
MYSQL_PASSWORD = 'my-secret-pw'
MYSQL_HOST = 'mysql'

# Establish the MySQL connection
mysql_connection = pymysql.connect(host=MYSQL_HOST,
                                   user=MYSQL_USER,
                                   password=MYSQL_PASSWORD,
                                   db=MYSQL_DB,
                                   charset='utf8mb4')

try:
    with mysql_connection.cursor() as cursor:
        # Write your SQL query here
        SQL_QUERY = """
        SELECT 
            L_SHIPMODE, 
            SUM(CASE WHEN O_ORDERPRIORITY = '1-URGENT' OR O_ORDERPRIORITY = '2-HIGH' THEN 1 ELSE 0 END) AS HIGH_PRIORITY_COUNT,
            SUM(CASE WHEN O_ORDERPRIORITY <> '1-URGENT' AND O_ORDERPRIORITY <> '2-HIGH' THEN 1 ELSE 0 END) AS LOW_PRIORITY_COUNT
        FROM 
            lineitem 
        JOIN 
            orders ON L_ORDERKEY = O_ORDERKEY
        WHERE 
            L_SHIPMODE IN ('MAIL', 'SHIP') 
            AND L_RECEIPTDATE BETWEEN '1994-01-01' AND '1995-01-01' 
            AND L_RECEIPTDATE > L_COMMITDATE 
            AND L_SHIPDATE < L_COMMITDATE
        GROUP BY 
            L_SHIPMODE
        ORDER BY 
            L_SHIPMODE ASC;
        """
        cursor.execute(SQL_QUERY)
        rows = cursor.fetchall()

        # Prepare the data for CSV
        output_data = [("ship_mode", "high_priority_count", "low_priority_count")] + list(rows)

        # Write to CSV
        with open('query_output.csv', 'w', newline='') as csvfile:
            csvwriter = csv.writer(csvfile)
            csvwriter.writerows(output_data)
finally:
    mysql_connection.close()
