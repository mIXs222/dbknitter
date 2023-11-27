# market_share_analysis.py

import pymysql
import csv
from datetime import datetime

# Connect to the MySQL database
connection = pymysql.connect(host='mysql',
                             user='root',
                             password='my-secret-pw',
                             database='tpch',
                             charset='utf8mb4')

try:
    with connection.cursor() as cursor:
        # SQL query
        query = """
        SELECT YEAR(O_ORDERDATE) AS order_year, 
               (SUM(CASE WHEN N_NAME = 'INDIA' THEN L_EXTENDEDPRICE * (1 - L_DISCOUNT) ELSE 0 END) /
               SUM(L_EXTENDEDPRICE * (1 - L_DISCOUNT))) AS market_share
        FROM lineitem
        JOIN orders ON L_ORDERKEY = O_ORDERKEY
        JOIN customer ON O_CUSTKEY = C_CUSTKEY
        JOIN nation ON C_NATIONKEY = N_NATIONKEY
        JOIN region ON N_REGIONKEY = R_REGIONKEY
        JOIN part ON L_PARTKEY = P_PARTKEY
        JOIN supplier ON L_SUPPKEY = S_SUPPKEY
        WHERE R_NAME = 'ASIA'
          AND P_TYPE = 'SMALL PLATED COPPER'
          AND N_NAME = 'INDIA'
          AND YEAR(O_ORDERDATE) BETWEEN 1995 AND 1996
        GROUP BY order_year
        ORDER BY order_year ASC;
        """
        
        cursor.execute(query)
        results = cursor.fetchall()

        # Write query results to CSV
        with open('query_output.csv', 'w', newline='') as csvfile:
            fieldnames = ['order_year', 'market_share']
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            for row in results:
                writer.writerow({'order_year': row[0], 'market_share': row[1]})

finally:
    connection.close()
