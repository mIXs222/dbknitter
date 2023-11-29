# query.py
import pymysql
import csv

# MySQL connection parameters
mysql_params = {
    'host': 'mysql',
    'user': 'root',
    'password': 'my-secret-pw',
    'database': 'tpch',
}

# SQL query
query = """
SELECT SUM(L_EXTENDEDPRICE * L_DISCOUNT) AS REVENUE
FROM lineitem
WHERE L_SHIPDATE > '1994-01-01' AND L_SHIPDATE < '1995-01-01'
AND L_DISCOUNT BETWEEN 0.06 - 0.01 AND 0.06 + 0.01
AND L_QUANTITY < 24;
"""

# Function to execute query and write output
def execute_query(connection_params, query_string, output_file):
    # Establish database connection
    connection = pymysql.connect(**connection_params)
    try:
        with connection.cursor() as cursor:
            cursor.execute(query_string)
            result = cursor.fetchall()
            
            # Write query result to csv file
            with open(output_file, mode='w', newline='') as file:
                csv_writer = csv.writer(file)
                csv_writer.writerow(['REVENUE'])  # Writing header
                for row in result:
                    csv_writer.writerow(row)
    except Exception as e:
        print(f'Error executing query: {e}')
    finally:
        # Close the database connection
        connection.close()

# Call function to execute query and write to CSV
execute_query(mysql_params, query, 'query_output.csv')
