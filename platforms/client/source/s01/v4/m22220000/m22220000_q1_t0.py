import pymysql
import csv

# Connection info for MySQL
mysql_conn_info = {
    'host': 'mysql',
    'user': 'root',
    'password': 'my-secret-pw',
    'database': 'tpch',
}

# Connect to the MySQL database using pymysql
mysql_conn = pymysql.connect(**mysql_conn_info)

try:
    with mysql_conn.cursor() as cursor:
        # SQL query
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
            L_LINESTATUS
        """

        # Execute the query
        cursor.execute(query)

        # Fetch all the results
        result_set = cursor.fetchall()
        
        # Header based on the select statement in the query
        header = ['L_RETURNFLAG', 'L_LINESTATUS', 'SUM_QTY', 'SUM_BASE_PRICE', 'SUM_DISC_PRICE',
                  'SUM_CHARGE', 'AVG_QTY', 'AVG_PRICE', 'AVG_DISC', 'COUNT_ORDER']

        # Write to CSV
        with open('query_output.csv', mode='w', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(header)
            for row in result_set:
                writer.writerow(row)

finally:
    # Close the MySQL connection
    mysql_conn.close()
