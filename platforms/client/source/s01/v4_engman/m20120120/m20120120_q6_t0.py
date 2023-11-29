import pymysql
import csv

# Define database connection properties
mysql_connection_config = {
    'host': 'mysql',
    'user': 'root',
    'password': 'my-secret-pw',
    'database': 'tpch'
}

# SQL query to be executed
query = """
SELECT SUM(L_EXTENDEDPRICE * L_DISCOUNT) AS REVENUE
FROM lineitem
WHERE L_SHIPDATE > '1994-01-01'
      AND L_SHIPDATE < '1995-01-01'
      AND L_DISCOUNT BETWEEN .06 - 0.01 AND .06 + 0.01
      AND L_QUANTITY < 24;
"""

# Connect to the MySQL database
conn = pymysql.connect(
    **mysql_connection_config
)

try:
    with conn.cursor() as cursor:
        # Execute the query
        cursor.execute(query)
        result = cursor.fetchone()
    
        # Write the output to a CSV file
        with open('query_output.csv', 'w', newline='') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(['REVENUE'])
            writer.writerow([result[0]])
finally:
    # Close the connection
    conn.close()
