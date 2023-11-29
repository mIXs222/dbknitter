import pymysql
import csv

# Connect to MySQL database
mysql_connection = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    database='tpch',
    charset='utf8mb4',
    cursorclass=pymysql.cursors.Cursor
)

try:
    with mysql_connection.cursor() as cursor:
        # Write SQL query based on user's description
        mysql_query = """
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
        FROM lineitem
        WHERE L_SHIPDATE < '1998-09-02'
        GROUP BY L_RETURNFLAG, L_LINESTATUS
        ORDER BY L_RETURNFLAG, L_LINESTATUS;
        """

        cursor.execute(mysql_query)
        results = cursor.fetchall()

        # Write the results to a CSV file
        with open('query_output.csv', 'w', newline='') as csvfile:
            fieldnames = [
                'L_RETURNFLAG', 'L_LINESTATUS', 'SUM_QTY', 'SUM_BASE_PRICE', 
                'SUM_DISC_PRICE', 'SUM_CHARGE', 'AVG_QTY', 'AVG_PRICE',
                'AVG_DISC', 'COUNT_ORDER'
            ]
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

            writer.writeheader()
            for row in results:
                writer.writerow({
                    'L_RETURNFLAG': row[0],
                    'L_LINESTATUS': row[1],
                    'SUM_QTY': row[2],
                    'SUM_BASE_PRICE': row[3],
                    'SUM_DISC_PRICE': row[4],
                    'SUM_CHARGE': row[5],
                    'AVG_QTY': row[6],
                    'AVG_PRICE': row[7],
                    'AVG_DISC': row[8],
                    'COUNT_ORDER': row[9]
                })
finally:
    mysql_connection.close()
