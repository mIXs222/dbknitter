import pymysql
import csv
from datetime import datetime, timedelta

# Define the connection details for MySQL database
mysql_details = {
    'host': 'mysql',
    'user': 'root',
    'password': 'my-secret-pw',
    'db': 'tpch',
}

# Establish a connection to MySQL database
mysql_connection = pymysql.connect(
    host=mysql_details['host'],
    user=mysql_details['user'],
    password=mysql_details['password'],
    db=mysql_details['db']
)

try:
    with mysql_connection.cursor() as cursor:
        # SQL query for MySQL
        seven_years_ago = (datetime.now() - timedelta(days=365 * 7)).date()
        mysql_query = f"""
            SELECT LEFT(C_PHONE, 2) AS country_code,
                   COUNT(*) AS number_of_customers,
                   AVG(C_ACCTBAL) AS average_balance
            FROM customer
            WHERE LEFT(C_PHONE, 2) IN ('20', '40', '22', '30', '39', '42', '21')
              AND C_ACCTBAL > 0
              AND C_CUSTKEY NOT IN (
                  SELECT O_CUSTKEY
                  FROM orders
                  WHERE O_ORDERDATE >= '{seven_years_ago}'
              )
            GROUP BY country_code;
        """

        # Execute the query
        cursor.execute(mysql_query)
        rows = cursor.fetchall()

        # Writing the result to CSV
        with open('query_output.csv', 'w', newline='') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(['country_code', 'number_of_customers', 'average_balance'])
            for row in rows:
                writer.writerow(row)

finally:
    mysql_connection.close()
