# query.py
import pymysql
import csv
from datetime import datetime

# Establish connection to the mysql database
connection = pymysql.connect(host='mysql',
                             user='root',
                             password='my-secret-pw',
                             database='tpch',
                             charset='utf8mb4',
                             cursorclass=pymysql.cursors.Cursor)

try:
    with connection.cursor() as cursor:
        # SQL query to get the required information
        query = """
            SELECT
                L_RETURNFLAG,
                L_LINESTATUS,
                SUM(L_QUANTITY) AS SUM_QTY,
                SUM(L_EXTENDEDPRICE) AS SUM_BASE_PRICE,
                SUM(L_EXTENDEDPRICE * (1 - L_DISCOUNT)) AS SUM_DISC_PRICE,
                SUM(L_EXTENDEDPRICE * (1 - L_DISCOUNT) * (1 + L_TAX)) AS SUM_CHARGE,
                AVG(L_QUANTITY) AS AVG_QTY,
                AVG(L_EXTENDEDPRICE) AS AVG_PRICE,
                AVG(L_DISCOUNT) AS AVG_DISC,
                COUNT(*) AS COUNT_ORDER
            FROM
                lineitem
            WHERE
                L_SHIPDATE < '1998-09-02'
            GROUP BY
                L_RETURNFLAG,
                L_LINESTATUS
            ORDER BY
                L_RETURNFLAG,
                L_LINESTATUS;
        """
        cursor.execute(query)
        
        # Fetch all the results
        results = cursor.fetchall()

        # Define the header
        header = [
            'L_RETURNFLAG', 'L_LINESTATUS', 'SUM_QTY', 'SUM_BASE_PRICE', 'SUM_DISC_PRICE',
            'SUM_CHARGE', 'AVG_QTY', 'AVG_PRICE', 'AVG_DISC', 'COUNT_ORDER'
        ]

        # Write results to CSV file
        with open('query_output.csv', 'w', newline='') as csvfile:
            csv_writer = csv.writer(csvfile)
            csv_writer.writerow(header)  # write header
            for row in results:
                csv_writer.writerow(row)

finally:
    # Close the connection to the database
    connection.close()
