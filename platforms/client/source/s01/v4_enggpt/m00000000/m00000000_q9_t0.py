# query_mysql.py
import pymysql
import csv
from datetime import datetime

# MySQL connection
mysql_conn = pymysql.connect(
    host="mysql",
    user="root",
    password="my-secret-pw",
    database="tpch"
)

try:
    with mysql_conn.cursor() as cursor:
        # The SQL query
        sql = """
        SELECT n.N_NAME as nation, YEAR(o.O_ORDERDATE) as year, 
               SUM((l.L_EXTENDEDPRICE * (1 - l.L_DISCOUNT)) - (ps.PS_SUPPLYCOST * l.L_QUANTITY)) as profit
        FROM nation n
        JOIN supplier s ON s.S_NATIONKEY = n.N_NATIONKEY
        JOIN partsupp ps ON ps.PS_SUPPKEY = s.S_SUPPKEY
        JOIN part p ON p.P_PARTKEY = ps.PS_PARTKEY AND p.P_NAME LIKE '%dim%'
        JOIN lineitem l ON l.L_PARTKEY = p.P_PARTKEY AND l.L_SUPPKEY = s.S_SUPPKEY
        JOIN orders o ON o.O_ORDERKEY = l.L_ORDERKEY
        GROUP BY nation, year
        ORDER BY nation ASC, year DESC
        """
        
        # Execute the query
        cursor.execute(sql)
        
        # Collect the results
        results = cursor.fetchall()
        
        # Write results to a CSV file
        with open('query_output.csv', 'w', newline='') as csvfile:
            fieldnames = ["nation", "year", "profit"]
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()

            for row in results:
                writer.writerow({
                    "nation": row[0],
                    "year": row[1],
                    "profit": row[2]
                })

finally:
    mysql_conn.close()
