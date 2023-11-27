import csv
import pymysql

# MySQL query
mysql_query = """
    SELECT
        L_RETURNFLAG,
        L_LINESTATUS,
        SUM(L_QUANTITY) AS sum_qty,
        SUM(L_EXTENDEDPRICE) AS sum_base_price,
        SUM(L_EXTENDEDPRICE * (1 - L_DISCOUNT)) AS sum_disc_price,
        SUM(L_EXTENDEDPRICE * (1 - L_DISCOUNT) * (1 + L_TAX)) AS sum_charge,
        AVG(L_QUANTITY) AS avg_qty,
        AVG(L_EXTENDEDPRICE) AS avg_price,
        AVG(L_DISCOUNT) AS avg_disc,
        COUNT(*) AS count_order
    FROM
        lineitem
    WHERE
        L_SHIPDATE < '1998-09-02'
    GROUP BY
        L_RETURNFLAG, L_LINESTATUS
    ORDER BY
        L_RETURNFLAG, L_LINESTATUS;
"""

# Database connection parameters
mysql_params = {
    "host": "mysql",
    "user": "root",
    "password": "my-secret-pw",
    "db": "tpch"
}

# Connect to MySQL
mysql_conn = pymysql.connect(**mysql_params)
mysql_cursor = mysql_conn.cursor()

# Execute the query in MySQL
mysql_cursor.execute(mysql_query)

# Get the query results
mysql_results = mysql_cursor.fetchall()

# Write query results to CSV file
with open('query_output.csv', mode='w', newline='') as file:
    writer = csv.writer(file)
    # Write header
    writer.writerow([
        'RETURNFLAG', 'LINESTATUS', 'SUM_QTY', 'SUM_BASE_PRICE', 'SUM_DISC_PRICE',
        'SUM_CHARGE', 'AVG_QTY', 'AVG_PRICE', 'AVG_DISC', 'COUNT_ORDER'
    ])
    # Write data rows
    for row in mysql_results:
        writer.writerow(row)

# Close connections
mysql_cursor.close()
mysql_conn.close()
