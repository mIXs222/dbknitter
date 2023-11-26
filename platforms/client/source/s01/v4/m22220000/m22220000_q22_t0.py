# query_script.py
import pymysql
import csv

# Connect to the MySQL database
connection = pymysql.connect(host='mysql',
                             user='root',
                             password='my-secret-pw',
                             database='tpch')

try:
    with connection.cursor() as cursor:
        # Execute the SQL query
        cursor.execute("""
        SELECT
            CNTRYCODE,
            COUNT(*) AS NUMCUST,
            SUM(C_ACCTBAL) AS TOTACCTBAL
        FROM
            (
            SELECT
                SUBSTR(C_PHONE, 1 , 2) AS CNTRYCODE,
                C_ACCTBAL
            FROM
                customer
            WHERE
                SUBSTR(C_PHONE , 1 , 2) IN
                ('20', '40', '22', '30', '39', '42', '21')
            AND C_ACCTBAL > (
                    SELECT
                        AVG(C_ACCTBAL)
                    FROM
                        customer
                    WHERE
                        C_ACCTBAL > 0.00
                    AND SUBSTR(C_PHONE , 1 , 2) IN
                    ('20', '40', '22', '30', '39', '42', '21')
            )
            AND NOT EXISTS (
                SELECT
                    *
                FROM
                    orders
                WHERE
                    O_CUSTKEY = C_CUSTKEY
                )
            ) AS CUSTSALE
        GROUP BY
            CNTRYCODE
        ORDER BY
            CNTRYCODE
        """)

        # Fetch the results
        results = cursor.fetchall()

        # Write the results to a CSV file
        with open('query_output.csv', 'w', newline='') as csvfile:
            csv_writer = csv.writer(csvfile)
            # Write the header
            csv_writer.writerow(['CNTRYCODE', 'NUMCUST', 'TOTACCTBAL'])
            # Write the data rows
            for row in results:
                csv_writer.writerow(row)

finally:
    connection.close()
