# query.py
import pymysql
import csv
from decimal import Decimal

# Establish connection to MySQL database
connection = pymysql.connect(host='mysql',
                             user='root',
                             password='my-secret-pw',
                             database='tpch',
                             charset='utf8mb4',
                             cursorclass=pymysql.cursors.Cursor)

try:
    with connection.cursor() as cursor:
        # Query to calculate market share for the years 1995 and 1996
        query = """
        SELECT year, SUM(IF(n.N_NAME = 'INDIA', revenue, 0)) / SUM(revenue) AS market_share
        FROM (
            SELECT o.O_ORDERDATE, l.L_EXTENDEDPRICE, l.L_DISCOUNT, s.S_NATIONKEY, n.N_NAME, n.N_REGIONKEY
            FROM lineitem l
            JOIN orders o ON l.L_ORDERKEY = o.O_ORDERKEY
            JOIN supplier s ON l.L_SUPPKEY = s.S_SUPPKEY
            JOIN nation n ON s.S_NATIONKEY = n.N_NATIONKEY
            JOIN region r ON n.N_REGIONKEY = r.R_REGIONKEY
            JOIN part p ON l.L_PARTKEY = p.P_PARTKEY
            WHERE r.R_NAME = 'ASIA'
            AND p.P_TYPE = 'SMALL PLATED COPPER'
            AND o.O_ORDERDATE BETWEEN '1995-01-01' AND '1996-12-31'
        ) AS join_tables
        CROSS JOIN (
            SELECT '1995-12-31' AS year_end
            UNION ALL
            SELECT '1996-12-31' AS year_end
        ) AS years
        INNER JOIN (
            SELECT '1995' AS year
            UNION ALL
            SELECT '1996' AS year
        ) AS years_labels ON join_tables.O_ORDERDATE <= years.year_end
        GROUP BY year
        ORDER BY year;
        """
        
        # Execute the query
        cursor.execute(query)
        
        # Fetch the results
        results = cursor.fetchall()

        # Writing query results to CSV
        with open('query_output.csv', 'w', newline='') as csvfile:
            csvwriter = csv.writer(csvfile)
            csvwriter.writerow(["year", "market_share"])
            for row in results:
                year, market_share = row
                if market_share is not None:
                    market_share = float(Decimal(market_share))
                csvwriter.writerow([year, market_share])

finally:
    # Close the connection
    connection.close()
