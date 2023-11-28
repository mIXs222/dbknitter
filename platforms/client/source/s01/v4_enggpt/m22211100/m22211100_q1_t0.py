import pymysql
import csv

# MySQL connection settings
mysql_settings = {
    "host": "mysql",
    "user": "root",
    "password": "my-secret-pw",
    "db": "tpch"
}

# Connect to MySQL database
mysql_conn = pymysql.connect(**mysql_settings)
mysql_cursor = mysql_conn.cursor()

# Query to summarize line items data
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
mysql_cursor.execute(query)

# Fetch the results
result = mysql_cursor.fetchall()

# Write the results to a CSV file
with open('query_output.csv', 'w', newline='') as f:
    writer = csv.writer(f)
    # Write header
    writer.writerow(['RETURNFLAG', 'LINESTATUS', 'SUM_QTY', 'SUM_BASE_PRICE', 'SUM_DISC_PRICE', 'SUM_CHARGE', 'AVG_QTY', 'AVG_PRICE', 'AVG_DISC', 'COUNT_ORDER'])
    # Write data
    for row in result:
        writer.writerow(row)

# Close the database connection
mysql_cursor.close()
mysql_conn.close()
