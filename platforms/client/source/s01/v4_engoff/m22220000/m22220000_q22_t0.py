# query.py

import pymysql
import csv
from datetime import datetime, timedelta

# MySQL connection details
MYSQL_HOST = 'mysql'
MYSQL_DB = 'tpch'
MYSQL_USER = 'root'
MYSQL_PASS = 'my-secret-pw'

# Connect to MySQL
connection = pymysql.connect(host=MYSQL_HOST,
                             user=MYSQL_USER,
                             password=MYSQL_PASS,
                             db=MYSQL_DB)

try:
    with connection.cursor() as cursor:
        
        # Construct the SQL Query
        sql = """
        SELECT 
            SUBSTRING(C_PHONE, 1, 2) AS country_code,
            COUNT(*) AS num_customers,
            AVG(C_ACCTBAL) AS avg_balance
        FROM 
            customer as c
        WHERE 
            C_ACCTBAL > 0.00 AND
            SUBSTRING(C_PHONE, 1, 2) IN ('20', '40', '22', '30', '39', '42', '21') AND
            NOT EXISTS (
                SELECT *
                FROM orders AS o
                WHERE o.O_CUSTKEY = c.C_CUSTKEY AND 
                      o.O_ORDERDATE >= %s
            )
        GROUP BY 
            country_code
        """
        
        # Calculate the date 7 years ago from today
        seven_years_ago = datetime.today() - timedelta(days=7*365)
        
        # Execute SQL Query
        cursor.execute(sql, (seven_years_ago,))
        
        # Fetch the result
        result = cursor.fetchall()
        
        # Write result to CSV
        with open('query_output.csv', mode='w') as file:
            writer = csv.writer(file, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
            writer.writerow(['country_code', 'num_customers', 'avg_balance'])
            for row in result:
                writer.writerow(row)

finally:
    connection.close()
