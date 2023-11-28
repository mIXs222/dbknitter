import pymysql
import csv

# Connect to the database
connection = pymysql.connect(host='mysql',
                             user='root',
                             password='my-secret-pw',
                             database='tpch')

try:
    with connection.cursor() as cursor:
        # SQL statement to execute
        sql_query = """
        SELECT CNTRYCODE, COUNT(*) AS NUMCUST, SUM(C_ACCTBAL) AS TOTACCTBAL
        FROM (
            SELECT SUBSTR(C_PHONE, 1, 2) AS CNTRYCODE, C_ACCTBAL
            FROM customer
            WHERE C_ACCTBAL > (SELECT AVG(C_ACCTBAL)
                                FROM customer
                                WHERE C_ACCTBAL > 0 AND SUBSTR(C_PHONE, 1, 2) IN ('20', '40', '22', '30', '39', '42', '21'))
              AND NOT EXISTS (SELECT * 
                              FROM orders
                              WHERE orders.O_CUSTKEY = customer.C_CUSTKEY)
        ) AS CUSTSALE
        GROUP BY CNTRYCODE
        ORDER BY CNTRYCODE ASC;
        """

        cursor.execute(sql_query)
        result = cursor.fetchall()

        # Write the results to a file
        with open('query_output.csv', 'w', newline='') as file:
            writer = csv.writer(file)
            writer.writerow(['CNTRYCODE', 'NUMCUST', 'TOTACCTBAL'])
            for row in result:
                writer.writerow(row)

finally:
    connection.close()
