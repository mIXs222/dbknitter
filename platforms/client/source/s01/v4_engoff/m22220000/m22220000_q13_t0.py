import pymysql
import csv

# Connect to the MySQL database
connection = pymysql.connect(host='mysql',
                             user='root',
                             password='my-secret-pw',
                             database='tpch')

try:
    with connection.cursor() as cursor:
        # SQL query for the customer and orders relationship
        sql = """
        SELECT c.C_CUSTKEY, COUNT(o.O_ORDERKEY) as num_orders
        FROM customer c
        LEFT JOIN orders o ON c.C_CUSTKEY = o.O_CUSTKEY AND o.O_ORDERSTATUS NOT IN ('pending', 'deposits')
        GROUP BY c.C_CUSTKEY;
        """
        cursor.execute(sql)

        # Fetch all the records and write them to a CSV file
        with open('query_output.csv', 'w', newline='') as csvfile:
            csvwriter = csv.writer(csvfile)
            csvwriter.writerow(['C_CUSTKEY', 'NUM_ORDERS'])

            for row in cursor.fetchall():
                csvwriter.writerow(row)

finally:
    connection.close()
