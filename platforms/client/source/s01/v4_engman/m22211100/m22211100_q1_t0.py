import csv
import pymysql

# Define MySQL connection function
def mysql_connection():
    return pymysql.connect(
        host='mysql',
        user='root',
        password='my-secret-pw',
        db='tpch',
    )

# Connect to MySQL
try:
    mysql_conn = mysql_connection()
    with mysql_conn.cursor() as cursor:
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
            L_SHIPDATE < '1998-09-02'
        GROUP BY
            L_RETURNFLAG,
            L_LINESTATUS
        ORDER BY
            L_RETURNFLAG,
            L_LINESTATUS;
        """
        cursor.execute(sql_query)
        results = cursor.fetchall()

    # Write query results to CSV file
    with open('query_output.csv', mode='w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(['RETURNFLAG', 'LINESTATUS', 'SUM_QTY', 'SUM_BASE_PRICE', 'SUM_DISC_PRICE', 'SUM_CHARGE', 'AVG_QTY', 'AVG_PRICE', 'AVG_DISC', 'COUNT_ORDER'])
        for row in results:
            writer.writerow(row)
            
except Exception as e:
    print("An error occurred:", e)

finally:
    # Close the MySQL connection
    if mysql_conn:
        mysql_conn.close()
