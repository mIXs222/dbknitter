# large_volume_customers.py
import csv
import pymysql

# Establishing the MySQL connection
connection = pymysql.connect(host='mysql',
                             user='root',
                             password='my-secret-pw',
                             database='tpch')

# SQL query to execute
query = """
SELECT c.C_NAME, c.C_CUSTKEY, o.O_ORDERKEY, o.O_ORDERDATE, o.O_TOTALPRICE, SUM(l.L_QUANTITY) AS total_quantity
FROM customer c
JOIN orders o ON c.C_CUSTKEY = o.O_CUSTKEY
JOIN lineitem l ON o.O_ORDERKEY = l.L_ORDERKEY
GROUP BY o.O_ORDERKEY
HAVING SUM(l.L_QUANTITY) > 300
ORDER BY o.O_TOTALPRICE DESC, o.O_ORDERDATE ASC
"""

try:
    with connection.cursor() as cursor:
        cursor.execute(query)
        
        # Writing query output to a CSV file
        with open('query_output.csv', 'w', newline='') as csvfile:
            csvwriter = csv.writer(csvfile)
            # Write header to the CSV file
            csvwriter.writerow([i[0] for i in cursor.description])
            # Write data to the CSV file
            for row in cursor.fetchall():
                csvwriter.writerow(row)

except Exception as e:
    print(f"An error occurred: {e}")

finally:
    connection.close()
