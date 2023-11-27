import pymysql
import csv

# Database configuration
config = {
    'host': 'mysql',
    'user': 'root',
    'password': 'my-secret-pw',
    'db': 'tpch'
}

# Establish a database connection
connection = pymysql.connect(**config)

try:
    with connection.cursor() as cursor:
        # SQL Query
        query = """
            SELECT SUM(L_EXTENDEDPRICE * L_DISCOUNT) AS potential_revenue_increase
            FROM lineitem
            WHERE L_SHIPDATE >= '1994-01-01'
            AND L_SHIPDATE < '1995-01-01'
            AND L_DISCOUNT BETWEEN 0.06 - 0.01 AND 0.06 + 0.01
            AND L_QUANTITY < 24;
        """
        # Execute the query
        cursor.execute(query)
        
        # Fetch the result
        result = cursor.fetchall()

        # Write to a CSV file
        with open('query_output.csv', mode='w', newline='') as file:
            writer = csv.writer(file)
            writer.writerow(['potential_revenue_increase'])
            for row in result:
                writer.writerow(row)

finally:
    connection.close()
