# Python code to execute the query on MySQL and write to query_output.csv
import pymysql
import csv

# MySQL connection parameters
mysql_db = 'tpch'
mysql_user = 'root'
mysql_password = 'my-secret-pw'
mysql_host = 'mysql'

# Connect to MySQL
connection = pymysql.connect(host=mysql_host, user=mysql_user, password=mysql_password, db=mysql_db)
try:
    with connection.cursor() as cursor:
        # Formulate the SQL query
        sql_query = """
        SELECT COUNT(*) AS num_orders, COUNT(DISTINCT C_CUSTKEY) AS num_customers
        FROM (
            SELECT O_CUSTKEY
            FROM orders
            WHERE O_ORDERSTATUS NOT IN ('P', 'D') AND O_COMMENT NOT LIKE '%pending%deposits%'
            GROUP BY O_CUSTKEY
        ) AS order_counts
        GROUP BY num_orders;
        """
        cursor.execute(sql_query)
        
        # Write to CSV
        with open('query_output.csv', mode='w', newline='') as file:
            csv_writer = csv.writer(file)
            csv_writer.writerow(['num_orders', 'num_customers'])
            for row in cursor.fetchall():
                csv_writer.writerow(row)
finally:
    connection.close()
