import pymysql
import csv
from datetime import datetime

# Connection information
connection_info = {
    'database': 'tpch',
    'user': 'root',
    'password': 'my-secret-pw',
    'host': 'mysql',
}

# Connect to the MySQL database
connection = pymysql.connect(
    host=connection_info['host'],
    user=connection_info['user'],
    password=connection_info['password'],
    database=connection_info['database'],
)

try:
    with connection.cursor() as cursor:
        # SQL query
        query = """
        SELECT 
            n.N_NAME AS nation,
            SUM(l.L_EXTENDEDPRICE * (1 - l.L_DISCOUNT)) AS revenue
        FROM 
            lineitem l
        JOIN 
            orders o ON l.L_ORDERKEY = o.O_ORDERKEY
        JOIN 
            customer c ON o.O_CUSTKEY = c.C_CUSTKEY
        JOIN 
            supplier s ON l.L_SUPPKEY = s.S_SUPPKEY
        JOIN 
            nation n ON s.S_NATIONKEY = n.N_NATIONKEY AND c.C_NATIONKEY = n.N_NATIONKEY
        JOIN 
            region r ON n.N_REGIONKEY = r.R_REGIONKEY
        WHERE 
            r.R_NAME = 'ASIA' AND
            o.O_ORDERDATE >= '1990-01-01' AND 
            o.O_ORDERDATE < '1995-01-01'
        GROUP BY nation
        ORDER BY revenue DESC;
        """

        cursor.execute(query)
        rows = cursor.fetchall()

        # Write results to CSV file
        with open('query_output.csv', 'w', newline='') as csvfile:
            fieldnames = ['nation', 'revenue']
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

            writer.writeheader()
            for row in rows:
                writer.writerow({'nation': row[0], 'revenue': row[1]})

finally:
    connection.close()
