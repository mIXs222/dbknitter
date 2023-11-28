import pymysql
import csv

# Establish connection to the MySQL database
connection = pymysql.connect(host='mysql',
                             user='root',
                             password='my-secret-pw',
                             database='tpch',
                             charset='utf8mb4',
                             cursorclass=pymysql.cursors.Cursor)

try:
    with connection.cursor() as cursor:
        # SQL query to fetch the required data
        query = """
        SELECT CNTRYCODE, COUNT(*) AS NUMCUST, SUM(C_ACCTBAL) AS TOTACCTBAL
        FROM (
            SELECT SUBSTR(C_PHONE, 1, 2) AS CNTRYCODE, C_ACCTBAL
            FROM customer
            WHERE C_ACCTBAL > (
                SELECT AVG(C_ACCTBAL)
                FROM customer
                WHERE C_ACCTBAL > 0 AND SUBSTR(C_PHONE, 1, 2) IN ('20', '40', '22', '30', '39', '42', '21')
            )
            AND NOT EXISTS (
                SELECT *
                FROM orders
                WHERE orders.O_CUSTKEY = customer.C_CUSTKEY
            )
        ) AS CUSTDATA
        GROUP BY CNTRYCODE
        ORDER BY CNTRYCODE ASC;
        """
        cursor.execute(query)
        rows = cursor.fetchall()
        
        # Write query results to CSV
        with open('query_output.csv', 'w', newline='') as csvfile:
            fieldnames = ['CNTRYCODE', 'NUMCUST', 'TOTACCTBAL']
            writer = csv.writer(csvfile)
            writer.writerow(fieldnames)
            for row in rows:
                writer.writerow(row)
finally:
    connection.close()
