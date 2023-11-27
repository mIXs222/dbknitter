import pymysql
import csv

# MySQL connection details
mysql_config = {
    'host': 'mysql',
    'user': 'root',
    'password': 'my-secret-pw',
    'db': 'tpch'
}

# Connect to MySQL
connection = pymysql.connect(**mysql_config)

try:
    with connection.cursor() as cursor:
        # SQL query
        sql = """
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

        cursor.execute(sql)
        result = cursor.fetchall()

        # Write to CSV file
        with open('query_output.csv', 'w', newline='') as csvfile:
            writer = csv.writer(csvfile)
            # Write header
            writer.writerow([i[0] for i in cursor.description])
            # Write data rows
            for row in result:
                writer.writerow(row)

finally:
    connection.close()
