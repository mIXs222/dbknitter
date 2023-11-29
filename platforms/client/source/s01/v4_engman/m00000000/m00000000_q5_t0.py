import pymysql
import csv
from decimal import Decimal

# Connect to the MySQL server
connection = pymysql.connect(host='mysql', user='root', password='my-secret-pw', db='tpch')
try:
    with connection.cursor() as cursor:
        # Execute the query    
        sql_query = """
        SELECT 
            n.N_NAME,
            SUM(l.L_EXTENDEDPRICE * (1 - l.L_DISCOUNT)) AS REVENUE
        FROM
            nation n
        JOIN 
            region r ON n.N_REGIONKEY = r.R_REGIONKEY AND r.R_NAME = 'ASIA'
        JOIN 
            supplier s ON n.N_NATIONKEY = s.S_NATIONKEY
        JOIN 
            lineitem l ON s.S_SUPPKEY = l.L_SUPPKEY
        JOIN 
            orders o ON l.L_ORDERKEY = o.O_ORDERKEY 
        JOIN 
            customer c ON o.O_CUSTKEY = c.C_CUSTKEY AND c.C_NATIONKEY = n.N_NATIONKEY
        WHERE 
            o.O_ORDERDATE >= '1990-01-01' AND o.O_ORDERDATE < '1995-01-01'
        GROUP BY 
            n.N_NAME
        ORDER BY 
            REVENUE DESC;
        """
        cursor.execute(sql_query)
        result = cursor.fetchall()

        # Write the result to a CSV file
        with open('query_output.csv', 'w', newline='') as csvfile:
            csvwriter = csv.writer(csvfile)
            csvwriter.writerow(['N_NAME', 'REVENUE'])
            for row in result:
                csvwriter.writerow([row[0], str(Decimal(row[1]))])
finally:
    connection.close()
