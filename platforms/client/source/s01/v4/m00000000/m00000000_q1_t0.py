import pymysql
import csv

# MySQL database connection information
mysql_config = {
    'user': 'root',
    'password': 'my-secret-pw',
    'host': 'mysql',
    'database': 'tpch'
}

# Connect to MySQL
mysql_conn = pymysql.connect(**mysql_config)
mysql_cursor = mysql_conn.cursor()

# MySQL query
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

# Execute the query in MySQL
mysql_cursor.execute(mysql_query)

# Get the results
results = mysql_cursor.fetchall()

# Write results to CSV
with open('query_output.csv', 'w', newline='') as csvfile:
    csv_writer = csv.writer(csvfile)
    # Write the header
    csv_writer.writerow([i[0] for i in mysql_cursor.description])
    # Write data rows
    for row in results:
        csv_writer.writerow(row)

# Close MySQL connections
mysql_cursor.close()
mysql_conn.close()
