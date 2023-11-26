import pymysql
import csv

# Establish connection to the MySQL database
connection = pymysql.connect(host='mysql',
                             user='root',
                             password='my-secret-pw',
                             database='tpch')
try:
    with connection.cursor() as cursor:
        # Execute the query
        cursor.execute("""
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
                L_SHIPDATE <= '1998-09-02'
            GROUP BY
                L_RETURNFLAG,
                L_LINESTATUS
            ORDER BY
                L_RETURNFLAG,
                L_LINESTATUS
        """)
        
        # Fetch all the results
        results = cursor.fetchall()
        
        # Define the header based on the query columns
        field_names = [
            'L_RETURNFLAG', 
            'L_LINESTATUS', 
            'SUM_QTY', 
            'SUM_BASE_PRICE', 
            'SUM_DISC_PRICE', 
            'SUM_CHARGE', 
            'AVG_QTY', 
            'AVG_PRICE', 
            'AVG_DISC', 
            'COUNT_ORDER'
        ]

        # Create or overwrite the 'query_output.csv' file with the results
        with open('query_output.csv', mode='w', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(field_names) # Write header
            for row in results:
                writer.writerow(row) # Write each row of data

finally:
    # Close the connection to the MySQL database
    connection.close()
