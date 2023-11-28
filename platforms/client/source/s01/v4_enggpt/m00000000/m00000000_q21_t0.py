# query_code.py

import pymysql
import csv

# Establish connection to the MySQL database
conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    db='tpch',
    charset='utf8mb4',
    cursorclass=pymysql.cursors.Cursor
)

try:
    with conn.cursor() as cursor:
        # Prepare the SQL query
        sql_query = """
        SELECT s.S_NAME, COUNT(*) as NUMWAIT
        FROM supplier s
        INNER JOIN nation n ON s.S_NATIONKEY = n.N_NATIONKEY
        INNER JOIN lineitem l1 ON s.S_SUPPKEY = l1.L_SUPPKEY
        INNER JOIN orders o ON l1.L_ORDERKEY = o.O_ORDERKEY
        WHERE n.N_NAME = 'SAUDI ARABIA'
        AND o.O_ORDERSTATUS = 'F'
        AND l1.L_RECEIPTDATE > l1.L_COMMITDATE
        AND EXISTS (
            SELECT 1
            FROM lineitem l2
            WHERE l1.L_ORDERKEY = l2.L_ORDERKEY
            AND l1.L_SUPPKEY != l2.L_SUPPKEY
        )
        AND NOT EXISTS (
            SELECT 1
            FROM lineitem l3
            WHERE l1.L_ORDERKEY = l3.L_ORDERKEY
            AND l1.L_SUPPKEY != l3.L_SUPPKEY
            AND l3.L_RECEIPTDATE > l3.L_COMMITDATE
        )
        GROUP BY s.S_NAME
        ORDER BY COUNT(*) DESC, s.S_NAME ASC;
        """
        # Execute the SQL query
        cursor.execute(sql_query)
        # Fetch all the results
        results = cursor.fetchall()
        # Writing the results to query_output.csv
        with open('query_output.csv', mode='w', newline='') as csv_file:
            writer = csv.writer(csv_file)
            writer.writerow(['S_NAME', 'NUMWAIT'])
            for row in results:
                writer.writerow(row)

finally:
    conn.close()
