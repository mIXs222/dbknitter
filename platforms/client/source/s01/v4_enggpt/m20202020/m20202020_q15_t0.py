import pymysql
import pandas as pd
import csv

# Connection info
mysql_conn_info = {
    'database': 'tpch',
    'user': 'root',
    'password': 'my-secret-pw',
    'host': 'mysql',
}

# Connect to MySQL
mysql_connection = pymysql.connect(host=mysql_conn_info['host'],
                                   user=mysql_conn_info['user'],
                                   password=mysql_conn_info['password'],
                                   db=mysql_conn_info['database'])

# Construct and execute the query
query = """
WITH revenue0 AS (
    SELECT
        L_SUPPKEY AS SUPPLIER_NO,
        SUM(L_EXTENDEDPRICE * (1 - L_DISCOUNT)) AS TOTAL_REVENUE
    FROM
        lineitem
    WHERE
        L_SHIPDATE BETWEEN '1996-01-01' AND '1996-03-31'
    GROUP BY
        L_SUPPKEY
)
SELECT
    S.S_SUPPKEY,
    S.S_NAME,
    S.S_ADDRESS,
    S.S_PHONE,
    R.TOTAL_REVENUE
FROM
    supplier AS S
JOIN
    (SELECT SUPPLIER_NO, TOTAL_REVENUE FROM revenue0 WHERE TOTAL_REVENUE = (SELECT MAX(TOTAL_REVENUE) FROM revenue0)) AS R
ON
    S.S_SUPPKEY = R.SUPPLIER_NO
ORDER BY
    S.S_SUPPKEY ASC;
"""

try:
    with mysql_connection.cursor() as cursor:
        cursor.execute(query)
        results = cursor.fetchall()
        # Write to CSV
        with open('query_output.csv', 'w', newline='') as csvfile:
            csv_writer = csv.writer(csvfile)
            # Write Header
            csv_writer.writerow([i[0] for i in cursor.description])
            # Write rows
            csv_writer.writerows(results)
finally:
    mysql_connection.close()
