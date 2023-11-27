import pymysql
import csv 

# Connect to the MySQL database
connection = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    database='tpch',
    cursorclass=pymysql.cursors.Cursor
)

try:
    with connection.cursor() as cursor:
        # The SQL Query to execute
        query = """
        SELECT CNTRYCODE, COUNT(*) AS NUMCUST, SUM(C_ACCTBAL) AS TOTACCTBAL
        FROM (
            SELECT SUBSTR(C_PHONE, 1, 2) AS CNTRYCODE, C_ACCTBAL
            FROM customer
            WHERE C_ACCTBAL > (
                SELECT AVG(C_ACCTBAL)
                FROM customer
                WHERE SUBSTR(C_PHONE, 1, 2) IN ('20', '40', '22', '30', '39', '42', '21') AND C_ACCTBAL > 0
            ) AND NOT EXISTS (
                SELECT O_CUSTKEY
                FROM orders
                WHERE orders.O_CUSTKEY = customer.C_CUSTKEY
            )
        ) AS CUSTSALE
        GROUP BY CNTRYCODE
        ORDER BY CNTRYCODE ASC;
        """
        
        # Execute the SQL query
        cursor.execute(query)
        
        # Fetch all the results
        results = cursor.fetchall()
        
        # Write to CSV file
        with open('query_output.csv', mode='w', newline='') as file:
            writer = csv.writer(file)
            writer.writerow(['CNTRYCODE', 'NUMCUST', 'TOTACCTBAL']) # Header
            for row in results:
                writer.writerow(row)

finally:
    # Close the connection
    connection.close()

