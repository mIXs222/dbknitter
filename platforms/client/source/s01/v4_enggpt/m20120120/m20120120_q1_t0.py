import pymysql
import csv

# Establish a MySQL connection
connection = pymysql.connect(host='mysql',
                             user='root',
                             password='my-secret-pw',
                             database='tpch')

try:
    with connection.cursor() as cursor:
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
            L_SHIPDATE <= '1998-09-02'
        GROUP BY
            L_RETURNFLAG,
            L_LINESTATUS
        ORDER BY
            L_RETURNFLAG,
            L_LINESTATUS;
        """
        cursor.execute(query)
        results = cursor.fetchall()

        # Write to CSV
        with open('query_output.csv', 'w', newline='') as csvfile:
            writer = csv.writer(csvfile)
            # Assuming the output should include headers
            writer.writerow(['RETURNFLAG', 'LINESTATUS', 'SUM_QTY', 'SUM_BASE_PRICE', 'SUM_DISC_PRICE', 'SUM_CHARGE',
                             'AVG_QTY', 'AVG_PRICE', 'AVG_DISC', 'COUNT_ORDER'])
            for row in results:
                writer.writerow(row)
finally:
    connection.close()
