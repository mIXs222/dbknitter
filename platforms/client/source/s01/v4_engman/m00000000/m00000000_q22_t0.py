import pymysql
import csv
from datetime import datetime, timedelta

# Calculate the date 7 years ago from today
seven_years_ago = datetime.now() - timedelta(days=7*365)

# Open connection to the MySQL database
conn = pymysql.connect(host='mysql', user='root', password='my-secret-pw', db='tpch', charset='utf8mb4', cursorclass=pymysql.cursors.Cursor)
try:
    with conn.cursor() as cursor:
        # Calculate the average account balance of customers whose balance is greater than 0
        avg_balance_query = """
        SELECT AVG(C_ACCTBAL) FROM customer WHERE C_ACCTBAL > 0.00;
        """
        cursor.execute(avg_balance_query)
        avg_balance = cursor.fetchone()[0]

        # Find customers matching the criteria and calculate number and total balance
        sales_opp_query = """
        SELECT LEFT(C_PHONE, 2) AS CNTRYCODE, COUNT(*) AS NUM_CUST, SUM(C_ACCTBAL) AS TOTAL_BALANCE
        FROM customer WHERE LEFT(C_PHONE, 2) IN ('20', '40', '22', '30', '39', '42', '21')
        AND C_CUSTKEY NOT IN (SELECT O_CUSTKEY FROM orders WHERE O_ORDERDATE > %s)
        AND C_ACCTBAL > %s
        GROUP BY CNTRYCODE ORDER BY CNTRYCODE ASC;
        """
        cursor.execute(sales_opp_query, (seven_years_ago.date(), avg_balance))
        results = cursor.fetchall()

        # Write the results to a CSV file
        with open('query_output.csv', mode='w', newline='') as file:
            writer = csv.writer(file)
            writer.writerow(['CNTRYCODE', 'NUM_CUST', 'TOTAL_BALANCE'])
            for row in results:
                writer.writerow(row)
finally:
    conn.close()
