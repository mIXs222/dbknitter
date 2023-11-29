import pymysql
import csv

# MySQL Connection
conn = pymysql.connect(host='mysql', user='root', passwd='my-secret-pw', db='tpch')
try:
    with conn.cursor() as cursor:
        query = """SELECT c.C_NAME, c.C_CUSTKEY, o.O_ORDERKEY, o.O_ORDERDATE, o.O_TOTALPRICE, SUM(l.L_QUANTITY) as total_quantity
                   FROM customer c
                   JOIN orders o ON c.C_CUSTKEY = o.O_CUSTKEY
                   JOIN lineitem l ON o.O_ORDERKEY = l.L_ORDERKEY
                   GROUP BY c.C_NAME, c.C_CUSTKEY, o.O_ORDERKEY, o.O_ORDERDATE, o.O_TOTALPRICE
                   HAVING SUM(l.L_QUANTITY) > 300
                   ORDER BY o.O_TOTALPRICE DESC, o.O_ORDERDATE ASC"""
        cursor.execute(query)
        results = cursor.fetchall()

        # Write results to CSV
        with open('query_output.csv', 'w', newline='') as csvfile:
            csvwriter = csv.writer(csvfile)
            csvwriter.writerow(['C_NAME', 'C_CUSTKEY', 'O_ORDERKEY', 'O_ORDERDATE', 'O_TOTALPRICE', 'total_quantity'])
            for row in results:
                csvwriter.writerow(row)
finally:
    conn.close()
