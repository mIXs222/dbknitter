# query.py
import csv
import pymysql
from datetime import datetime, timedelta

# Database connection information
db_info = {
    'mysql': {
        'database': 'tpch',
        'user': 'root',
        'password': 'my-secret-pw',
        'host': 'mysql'
    }
}

# Establish the MySQL connection
mysql_conn = pymysql.connect(host=db_info['mysql']['host'],
                             user=db_info['mysql']['user'],
                             password=db_info['mysql']['password'],
                             db=db_info['mysql']['database'])

try:
    with mysql_conn.cursor() as cursor:
        # Calculate the date threshold for orders (current date - 7 years)
        seven_years_ago = datetime.now() - timedelta(days=7*365)

        # The SQL query
        sql = """
            SELECT
                SUBSTRING(c_phone, 1, 2) AS CNTRYCODE,
                COUNT(*) AS num_customers,
                SUM(c_acctbal) AS total_acctbal
            FROM
                customer
            WHERE
                c_phone LIKE '20%' OR
                c_phone LIKE '40%' OR
                c_phone LIKE '22%' OR
                c_phone LIKE '30%' OR
                c_phone LIKE '39%' OR
                c_phone LIKE '42%' OR
                c_phone LIKE '21%'
            GROUP BY
                CNTRYCODE
            HAVING
                SUM(c_acctbal) > 0 AND
                NOT EXISTS (
                    SELECT 1
                    FROM orders
                    WHERE customer.c_custkey = orders.o_custkey AND
                    o_orderdate > %s
                )
            ORDER BY
                CNTRYCODE ASC;
        """
        cursor.execute(sql, (seven_years_ago,))

        # Fetch all the results
        results = cursor.fetchall()

        # Write results to a CSV file
        with open('query_output.csv', 'w', newline='') as csvfile:
            csv_writer = csv.writer(csvfile)
            csv_writer.writerow(['CNTRYCODE', 'num_customers', 'total_acctbal'])
            for row in results:
                csv_writer.writerow(row)
finally:
    mysql_conn.close()
