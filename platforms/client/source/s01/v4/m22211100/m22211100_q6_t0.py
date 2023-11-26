# db_query.py
import pymysql
import csv

# Connection parameters
connection_params = {
    "database": "tpch",
    "user": "root",
    "password": "my-secret-pw",
    "host": "mysql"
}

# SQL query
sql_query = """
SELECT
    SUM(L_EXTENDEDPRICE * L_DISCOUNT) AS REVENUE
FROM
    lineitem
WHERE
    L_SHIPDATE >= '1994-01-01'
    AND L_SHIPDATE < '1995-01-01'
    AND L_DISCOUNT BETWEEN .06 - 0.01 AND .06 + 0.01
    AND L_QUANTITY < 24
"""

# Execute the query and write the output to a CSV file
try:
    # Connect to the database
    connection = pymysql.connect(**connection_params)

    with connection.cursor() as cursor:
        # Execute the SQL query
        cursor.execute(sql_query)
        
        # Fetch the result
        result = cursor.fetchall()

        # Write the output to the CSV file
        with open("query_output.csv", "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(["REVENUE"])  # header
            writer.writerows(result)
finally:
    if connection:
        # Close the connection
        connection.close()
