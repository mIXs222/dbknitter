import pymysql
import csv

# Define database connection parameters
db_config = {
    'database': 'tpch',
    'user': 'root',
    'password': 'my-secret-pw',
    'host': 'mysql',
} 

# SQL Query
query = """
SELECT SUM(L_EXTENDEDPRICE * L_DISCOUNT) AS REVENUE
FROM lineitem
WHERE L_SHIPDATE > '1994-01-01' AND L_SHIPDATE < '1995-01-01'
AND L_DISCOUNT BETWEEN 0.05 AND 0.07 AND L_QUANTITY < 24;
"""

# Connect to the MySQL database
connection = pymysql.connect(
    db=db_config['database'],
    user=db_config['user'],
    password=db_config['password'],
    host=db_config['host']
)

try:
    with connection.cursor() as cursor:
        # Execute the query
        cursor.execute(query)

        # Fetch the result
        result = cursor.fetchall()

        # Write the result to a CSV file
        with open('query_output.csv', mode='w', newline='') as file:
            writer = csv.writer(file)
            writer.writerow(['REVENUE'])  # write header
            for row in result:
                writer.writerow(row)
finally:
    connection.close()
