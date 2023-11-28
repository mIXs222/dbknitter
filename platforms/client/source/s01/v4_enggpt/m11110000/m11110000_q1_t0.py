import pymysql
import csv

# MySQL connection configs
mysql_config = {
    'database': 'tpch',
    'user': 'root',
    'password': 'my-secret-pw',
    'host': 'mysql'
}

# Connect to the MySQL database
mysql_conn = pymysql.connect(
    host=mysql_config['host'],
    user=mysql_config['user'],
    password=mysql_config['password'],
    db=mysql_config['database']
)

try:
    with mysql_conn.cursor() as cursor:
        # Execute the analysis query
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

        cursor.execute(sql_query)
        result_set = cursor.fetchall()

    # Write the results to a CSV file
    with open('query_output.csv', 'w', newline='') as csvfile:
        csvwriter = csv.writer(csvfile)
        # Write Header
        csvwriter.writerow([
            'L_RETURNFLAG', 'L_LINESTATUS', 'SUM_QTY', 'SUM_BASE_PRICE', 
            'SUM_DISC_PRICE', 'SUM_CHARGE', 'AVG_QTY', 'AVG_PRICE', 
            'AVG_DISC', 'COUNT_ORDER'
        ])
        # Write data rows
        for row in result_set:
            csvwriter.writerow(row)

finally:
    mysql_conn.close()
