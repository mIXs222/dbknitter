import csv
import pymysql

# Define connection information
connection_info = {
    'host': 'mysql',
    'user': 'root',
    'password': 'my-secret-pw',
    'db': 'tpch',
    'charset': 'utf8mb4'
}

# Connect to MySQL database
connection = pymysql.connect(**connection_info)

# Define the query
query = '''
SELECT
    L_RETURNFLAG,
    L_LINESTATUS,
    SUM(L_QUANTITY) AS SUM_QTY,
    SUM(L_EXTENDEDPRICE) AS SUM_BASE_PRICE,
    SUM(L_EXTENDEDPRICE*(1-L_DISCOUNT)) AS SUM_DISC_PRICE,
    SUM(L_EXTENDEDPRICE*(1-L_DISCOUNT)*(1+L_TAX)) AS SUM_CHARGE,
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
'''

try:
    with connection.cursor() as cursor:
        # Execute the query
        cursor.execute(query)
        # Fetch all the records
        result = cursor.fetchall()
        # Write the fetched records to CSV file
        with open('query_output.csv', mode='w', newline='') as file:
            csv_writer = csv.writer(file)
            # Writing headers
            csv_writer.writerow([i[0] for i in cursor.description])
            # Writing rows
            for row in result:
                csv_writer.writerow(row)
finally:
    # Close the connection
    connection.close()
