import csv
import pymysql

# MySQL connection
connection = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    database='tpch'
)

try:
    with connection.cursor() as cursor:
        query = """
        SELECT c.C_NAME, c.C_CUSTKEY, o.O_ORDERKEY, o.O_ORDERDATE, o.O_TOTALPRICE, SUM(l.L_QUANTITY) AS TOTAL_QUANTITY
        FROM customer c
        JOIN orders o ON c.C_CUSTKEY = o.O_CUSTKEY
        JOIN lineitem l ON o.O_ORDERKEY = l.L_ORDERKEY
        GROUP BY c.C_NAME, c.C_CUSTKEY, o.O_ORDERKEY, o.O_ORDERDATE, o.O_TOTALPRICE
        HAVING SUM(l.L_QUANTITY) > 300
        ORDER BY o.O_TOTALPRICE DESC, o.O_ORDERDATE;
        """
        cursor.execute(query)
        rows = cursor.fetchall()
        
        # Write to CSV
        with open('query_output.csv', mode='w', newline='') as file:
            writer = csv.writer(file)
            # Write column headers
            writer.writerow(['C_NAME', 'C_CUSTKEY', 'O_ORDERKEY', 'O_ORDERDATE', 'O_TOTALPRICE', 'TOTAL_QUANTITY'])
            # Write rows of data
            for row in rows:
                writer.writerow(row)

finally:
    connection.close()
