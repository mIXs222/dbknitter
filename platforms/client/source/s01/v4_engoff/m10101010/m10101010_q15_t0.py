import pymysql
import csv
from datetime import datetime

# Connect to the MySQL database
conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    database='tpch'
)

try:
    with conn.cursor() as cursor:
        # SQL statement to find the top supplier based on the criteria
        query = """
        SELECT s.S_SUPPKEY, s.S_NAME, s.S_ADDRESS, s.S_PHONE, SUM(l.L_EXTENDEDPRICE * (1 - l.L_DISCOUNT)) AS total_revenue
        FROM supplier s
        JOIN lineitem l ON s.S_SUPPKEY = l.L_SUPPKEY
        WHERE l.L_SHIPDATE >= '1996-01-01' AND l.L_SHIPDATE < '1996-04-01'
        GROUP BY s.S_SUPPKEY, s.S_NAME, s.S_ADDRESS, s.S_PHONE
        ORDER BY total_revenue DESC, s.S_SUPPKEY
        """
        cursor.execute(query)

        # Retrieve the results
        results = cursor.fetchall()

        # Find the maximum revenue
        max_revenue = results[0][4] if results else 0

        # Filter for suppliers with maximum revenue
        top_suppliers = [row for row in results if row[4] == max_revenue]

        # Write the output to a CSV file
        with open('query_output.csv', mode='w', newline='') as file:
            writer = csv.writer(file)
            # Write the headers
            writer.writerow(['S_SUPPKEY', 'S_NAME', 'S_ADDRESS', 'S_PHONE', 'TOTAL_REVENUE'])
            # Write the data
            for row in top_suppliers:
                writer.writerow(row)

except Exception as e:
    print(f"An error occurred: {e}")

finally:
    # Close the database connection
    conn.close()
