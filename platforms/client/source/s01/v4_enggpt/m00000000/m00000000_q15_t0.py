import pymysql
import csv
from datetime import datetime

# Establish a connection to the MySQL database
connection = pymysql.connect(host='mysql',
                             user='root',
                             password='my-secret-pw',
                             database='tpch')

# Prepare the query
query = """
WITH revenue0 AS (
    SELECT 
        L_SUPPKEY AS SUPPLIER_NO,
        SUM(L_EXTENDEDPRICE * (1 - L_DISCOUNT)) AS TOTAL_REVENUE
    FROM 
        lineitem
    WHERE 
        L_SHIPDATE >= '1996-01-01' AND L_SHIPDATE <= '1996-03-31'
    GROUP BY 
        L_SUPPKEY
)
SELECT 
    s.S_SUPPKEY, 
    s.S_NAME,
    s.S_ADDRESS,
    s.S_PHONE,
    r.TOTAL_REVENUE
FROM 
    supplier s
JOIN 
    revenue0 r ON s.S_SUPPKEY = r.SUPPLIER_NO
ORDER BY 
    r.TOTAL_REVENUE DESC
LIMIT 1;
"""

try:
    with connection.cursor() as cursor:
        cursor.execute(query)
        result = cursor.fetchall()

        # Write query output to CSV file
        with open('query_output.csv', 'w', newline='') as csvfile:
            csvwriter = csv.writer(csvfile)
            # Write header
            csvwriter.writerow(['S_SUPPKEY', 'S_NAME', 'S_ADDRESS', 'S_PHONE', 'TOTAL_REVENUE'])
            # Write data
            for row in result:
                csvwriter.writerow(row)
finally:
    connection.close()
