import pymysql
import csv
from datetime import date

# Connect to the MySQL database
connection = pymysql.connect(host='mysql',
                             user='root',
                             password='my-secret-pw',
                             database='tpch')

try:
    with connection.cursor() as cursor:
        
        # The SQL query:
        sql = """
            SELECT 
              SUM(IF(P_PROMOFLAG='PROMO', l_extendedprice * (1-l_discount), 0)) / SUM(l_extendedprice * (1-l_discount)) * 100 as promo_revenue_percentage
            FROM 
              part, 
              lineitem
            WHERE 
              L_PARTKEY = P_PARTKEY
              AND L_SHIPDATE >= '1995-09-01'
              AND L_SHIPDATE < '1995-10-01'
              AND P_PROMOFLAG IS NOT NULL;
        """
        
        # Execute the SQL query
        cursor.execute(sql)
        
        # Fetch the results
        result = cursor.fetchone()
        
        # Write the results to a CSV file
        with open('query_output.csv', 'w', newline='') as csvfile:
            csvwriter = csv.writer(csvfile)
            csvwriter.writerow(['promo_revenue_percentage'])
            csvwriter.writerow([result['promo_revenue_percentage'] if result['promo_revenue_percentage'] is not None else 'No promotional parts shipped in this period'])
            
finally:
    connection.close()
