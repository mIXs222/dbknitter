import pymysql
import csv

# Connection details
connection_params = {
    'host': 'mysql',
    'user': 'root',
    'password': 'my-secret-pw',
    'db': 'tpch',
    'charset': 'utf8mb4',
    'cursorclass': pymysql.cursors.Cursor,
}

# SQL Query
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
    L_LINESTATUS;
"""

# Connect to the database
connection = pymysql.connect(**connection_params)

try:
    with connection.cursor() as cursor:
        cursor.execute(sql_query)
        result = cursor.fetchall()

        # Write result to csv file
        with open('query_output.csv', 'w', newline='') as csvfile:
            csv_writer = csv.writer(csvfile)
            # Writing header
            csv_writer.writerow([i[0] for i in cursor.description])
            # Writing data
            csv_writer.writerows(result)
finally:
    connection.close()
