import pymysql
import csv

# Define connection parameters
db_params = {
    'host': 'mysql',
    'user': 'root',
    'password': 'my-secret-pw',
    'db': 'tpch',
    'charset': 'utf8mb4'
}

# Connect to MySQL
connection = pymysql.connect(**db_params)
try:
    with connection.cursor() as cursor:
        # SQL query to execute
        query = '''
            SELECT c.C_NAME, c.C_CUSTKEY, o.O_ORDERKEY, o.O_ORDERDATE, o.O_TOTALPRICE, SUM(l.L_QUANTITY) as total_quantity
            FROM customer c
            JOIN orders o ON c.C_CUSTKEY = o.O_CUSTKEY
            JOIN lineitem l ON o.O_ORDERKEY = l.L_ORDERKEY
            GROUP BY c.C_CUSTKEY, o.O_ORDERKEY
            HAVING total_quantity > 300
        '''

        cursor.execute(query)
        result = cursor.fetchall()

        # Write to CSV file
        with open('query_output.csv', 'w', newline='') as csvfile:
            csvwriter = csv.writer(csvfile)
            # Write the header
            csvwriter.writerow(['C_NAME', 'C_CUSTKEY', 'O_ORDERKEY', 'O_ORDERDATE', 'O_TOTALPRICE', 'total_quantity'])
            # Write the data
            for row in result:
                csvwriter.writerow(row)
finally:
    connection.close()
