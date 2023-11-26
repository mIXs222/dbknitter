import pymysql
import csv

# Connection parameters
db_config = {
    "host": "mysql",
    "user": "root",
    "password": "my-secret-pw",
    "database": "tpch",
}

# SQL query
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
    L_LINESTATUS
"""

try:
    # Connect to the database
    connection = pymysql.connect(**db_config)
    with connection.cursor() as cursor:
        cursor.execute(sql_query)
        data = cursor.fetchall()

        # Write output to CSV file
        with open('query_output.csv', mode='w', newline='') as outfile:
            csv_writer = csv.writer(outfile)
            csv_writer.writerow([i[0] for i in cursor.description])  # Write headers
            for row in data:
                csv_writer.writerow(row)  # Write data
finally:
    if connection:
        connection.close()
