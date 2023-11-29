# query.py
import csv
import pymysql.cursors

# Connection information for mysql
mysql_conn_info = {
    'host': 'mysql',
    'user': 'root',
    'password': 'my-secret-pw',
    'database': 'tpch',
}

# SQL query to be executed
sql_query = """
SELECT
    L_RETURNFLAG,
    L_LINESTATUS,
    SUM(L_QUANTITY) AS SUM_QTY,
    SUM(L_EXTENDEDPRICE) AS SUM_BASE_PRICE,
    SUM(L_EXTENDEDPRICE * (1 - L_DISCOUNT)) AS SUM_DISC_PRICE,
    SUM(L_EXTENDEDPRICE * (1 - L_DISCOUNT) * (1 + L_TAX)) AS SUM_CHARGE,
    AVG(L_QUANTITY) AS AVG_QTY,
    AVG(L_EXTENDEDPRICE) AS AVG_PRICE,
    AVG(L_DISCOUNT) AS AVG_DISCOUNT,
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

# Establish a connection to the mysql database
connection = pymysql.connect(**mysql_conn_info)

try:
    with connection.cursor() as cursor:
        # Execute the query
        cursor.execute(sql_query)
        results = cursor.fetchall()
        
        # Write the query's output to query_output.csv
        with open('query_output.csv', mode='w') as file:
            csv_writer = csv.writer(file)
            # Write the headers
            csv_writer.writerow([i[0] for i in cursor.description])
            # Write the data
            for row in results:
                csv_writer.writerow(row)
finally:
    connection.close()
