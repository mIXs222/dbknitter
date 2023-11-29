# query.py

import pymysql
import csv

# MySQL connection setup
db_settings = {
    "host": "mysql",
    "user": "root",
    "password": "my-secret-pw",
    "database": "tpch",
}

# The SQL to execute
sql = """
SELECT SUM(L_EXTENDEDPRICE * L_DISCOUNT) AS REVENUE
FROM lineitem
WHERE L_SHIPDATE > '1994-01-01' AND L_SHIPDATE < '1995-01-01'
AND L_DISCOUNT BETWEEN 0.05 AND 0.07
AND L_QUANTITY < 24;
"""

def execute_query(sql):
    # Connect to the MySQL database
    connection = pymysql.connect(**db_settings)

    try:
        with connection.cursor() as cursor:
            # Execute SQL
            cursor.execute(sql)

            # Fetch the results
            result = cursor.fetchone()

            # Write results to CSV
            with open('query_output.csv', 'w', newline='') as csvfile:
                csv_writer = csv.writer(csvfile)
                csv_writer.writerow(['REVENUE'])
                csv_writer.writerow(result)

    finally:
        # Close the connection
        connection.close()

if __name__ == "__main__":
    execute_query(sql)
