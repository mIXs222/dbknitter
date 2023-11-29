import pymysql
import csv

# Establish a connection to the MySQL server
connection = pymysql.connect(host='mysql',
                             user='root',
                             password='my-secret-pw',
                             database='tpch',
                             charset='utf8mb4')

try:
    with connection.cursor() as cursor:
        query = """
        SELECT 
            YEAR(o_orderdate) as year, 
            SUM( 
                CASE 
                    WHEN n_name = 'INDIA' THEN l_extendedprice * (1 - l_discount) 
                    ELSE 0 
                END 
            ) / SUM(l_extendedprice * (1 - l_discount)) as market_share
        FROM 
            lineitem
        JOIN orders ON l_orderkey = o_orderkey 
        JOIN part ON l_partkey = p_partkey
        JOIN supplier ON l_suppkey = s_suppkey
        JOIN nation ON s_nationkey = n_nationkey
        JOIN region ON n_regionkey = r_regionkey
        WHERE 
            r_name = 'ASIA'
            AND p_type = 'SMALL PLATED COPPER'
            AND YEAR(o_orderdate) IN (1995, 1996)
        GROUP BY YEAR(o_orderdate)
        ORDER BY year;
        """
        cursor.execute(query)
        results = cursor.fetchall()
        
        # Write results to CSV file
        with open('query_output.csv', 'w', newline='') as csvfile:
            csvwriter = csv.writer(csvfile)
            csvwriter.writerow(['order_year', 'market_share'])  # Column headers
            for row in results:
                csvwriter.writerow(row)

finally:
    connection.close()
