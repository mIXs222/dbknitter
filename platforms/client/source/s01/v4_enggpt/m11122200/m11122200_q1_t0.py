import pymysql
import pandas as pd
import csv

# Connection info
mysql_conn_info = {
    'database': 'tpch',
    'user': 'root',
    'password': 'my-secret-pw',
    'host': 'mysql',
}

# Connect to MySQL database
mysql_connection = pymysql.connect(host=mysql_conn_info['host'],
                                   user=mysql_conn_info['user'],
                                   password=mysql_conn_info['password'],
                                   db=mysql_conn_info['database'])

try:
    mysql_cursor = mysql_connection.cursor()

    # Write the query
    mysql_query = '''
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
        L_RETURNFLAG, L_LINESTATUS
    ORDER BY
        L_RETURNFLAG, L_LINESTATUS;
    '''

    # Execute the query
    mysql_cursor.execute(mysql_query)

    # Fetch the results
    results = mysql_cursor.fetchall()

    # Define the column names
    col_names = [i[0] for i in mysql_cursor.description]

    # Write results to a CSV file
    with open('query_output.csv', 'w', newline='') as file:
        csv_writer = csv.writer(file)
        csv_writer.writerow(col_names)
        csv_writer.writerows(results)

finally:
    mysql_cursor.close()
    mysql_connection.close()
