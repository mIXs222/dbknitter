import pymysql
import csv
from datetime import datetime, timedelta

# Establishing the MySQL connection
connection = pymysql.connect(host='mysql', user='root', password='my-secret-pw', database='tpch')

# Prepare the SQL query
query = """
    SELECT 
        SUBSTRING(C_PHONE, 1, 2) AS CNTRYCODE,
        COUNT(*) AS num_customers,
        SUM(C_ACCTBAL) AS total_acct_balance
    FROM customer
    WHERE
        SUBSTRING(C_PHONE, 1, 2) IN ('20', '40', '22', '30', '39', '42', '21')
        AND C_CUSTKEY NOT IN (
            SELECT O_CUSTKEY
            FROM orders
            WHERE O_ORDERDATE >= DATE_SUB(NOW(), INTERVAL 7 YEAR)
        )
        AND C_ACCTBAL > 0.00
        AND C_ACCTBAL > (
            SELECT AVG(C_ACCTBAL)
            FROM customer
            WHERE C_ACCTBAL > 0.00
                AND SUBSTRING(C_PHONE, 1, 2) = SUBSTRING(customer.C_PHONE, 1, 2)
        )
    GROUP BY CNTRYCODE
    ORDER BY CNTRYCODE;
"""

try:
    with connection.cursor() as cursor:
        # Execute the SQL command
        cursor.execute(query)
        
        # Fetch all the rows
        results = cursor.fetchall()
        
        # Write to CSV
        with open('query_output.csv', 'w', newline='') as csvfile:
            csvwriter = csv.writer(csvfile)
            # Header
            csvwriter.writerow(['CNTRYCODE', 'num_customers', 'total_acct_balance'])
            # Data
            for row in results:
                csvwriter.writerow(row)

finally:
    # Close the connection
    connection.close()
