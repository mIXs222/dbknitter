# query.py

import pymysql
import csv

# MySQL connection parameters
mysql_config = {
    'database': 'tpch',
    'user': 'root',
    'password': 'my-secret-pw',
    'host': 'mysql',
}

# Connect to MySQL
mysql_conn = pymysql.connect(**mysql_config)
mysql_cursor = mysql_conn.cursor()

# Merge query
merge_query = """
    SELECT
        C_NAME,
        C_CUSTKEY,
        O_ORDERKEY,
        O_ORDERDATE,
        O_TOTALPRICE,
        SUM(L_QUANTITY) as total_quantity
    FROM
        customer,
        orders,
        lineitem
    WHERE
        O_ORDERKEY IN (
            SELECT
                L_ORDERKEY
            FROM
                lineitem
            GROUP BY
                L_ORDERKEY
            HAVING
                SUM(L_QUANTITY) > 300
        )
    AND C_CUSTKEY = O_CUSTKEY
    AND O_ORDERKEY = L_ORDERKEY
    GROUP BY
        C_NAME,
        C_CUSTKEY,
        O_ORDERKEY,
        O_ORDERDATE,
        O_TOTALPRICE
    ORDER BY
        O_TOTALPRICE DESC,
        O_ORDERDATE
"""

try:
    # Execute the query on MySQL
    mysql_cursor.execute(merge_query)
    results = mysql_cursor.fetchall()
    
    # Write the results to a CSV file
    with open('query_output.csv', 'w', newline='', encoding='utf-8') as csvfile:
        csvwriter = csv.writer(csvfile)
        # Write header
        csvwriter.writerow(['C_NAME', 'C_CUSTKEY', 'O_ORDERKEY', 'O_ORDERDATE', 'O_TOTALPRICE', 'SUM(L_QUANTITY)'])
        # Write data
        for row in results:
            csvwriter.writerow(row)

finally:
    # Close the MySQL connection
    mysql_cursor.close()
    mysql_conn.close()
