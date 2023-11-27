import pymysql
import csv

# Define the connection details
mysql_conn_details = {
    'host': 'mysql',
    'user': 'root',
    'password': 'my-secret-pw',
    'db': 'tpch',
    'cursorclass': pymysql.cursors.Cursor
}

# SQL query to execute
sql_query = """
SELECT
    o.O_ORDERPRIORITY,
    SUM(l.L_EXTENDEDPRICE * (1 - l.L_DISCOUNT)) AS revenue
FROM
    orders o
INNER JOIN
    lineitem l ON o.O_ORDERKEY = l.L_ORDERKEY
INNER JOIN
    customer c ON o.O_CUSTKEY = c.C_CUSTKEY
WHERE
    o.O_ORDERDATE < '1995-03-15'
    AND l.L_SHIPDATE > '1995-03-15'
    AND c.C_MKTSEGMENT = 'BUILDING'
GROUP BY 
    o.O_ORDERPRIORITY
ORDER BY 
    revenue DESC
"""

# Function to connect to the MySQL database and execute the query
def execute_query_and_save_to_csv(query, conn_details, output_file):
    try:
        # Open the connection to the database
        connection = pymysql.connect(**conn_details)
        
        # Create a cursor object
        cursor = connection.cursor()
        
        # Execute the SQL query
        cursor.execute(query)
        
        # Fetch all the results
        query_results = cursor.fetchall()
        
        # Write results to the CSV file
        with open(output_file, 'w', newline='') as file:
            csv_writer = csv.writer(file)
            csv_writer.writerow(['O_ORDERPRIORITY', 'Revenue'])  # Write header
            for row in query_results:
                csv_writer.writerow(row)

    except Exception as e:
        print(f"Error: {str(e)}")
    finally:
        # Ensure that the connection is closed
        if connection:
            cursor.close()
            connection.close()

# Call the function to execute the query and save the results to a CSV file
execute_query_and_save_to_csv(sql_query, mysql_conn_details, 'query_output.csv')
