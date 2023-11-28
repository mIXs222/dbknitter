import pymysql
import csv
from datetime import datetime

# MySQL connection function
def connect_to_mysql(host, user, password, db):
    return pymysql.connect(host=host,
                           user=user,
                           password=password,
                           db=db,
                           charset='utf8mb4')

# Query execution function
def execute_query(connection, query):
    with connection.cursor() as cursor:
        cursor.execute(query)
        return cursor.fetchall()

# Main script
def main():
    # Database connection details
    mysql_db_details = {
        'host': 'mysql',
        'user': 'root',
        'password': 'my-secret-pw',
        'db': 'tpch'
    }

    # Connect to MySQL
    mysql_connection = connect_to_mysql(**mysql_db_details)

    # SQL query
    query = """
    SELECT n.N_NAME, SUM(l.L_EXTENDEDPRICE * (1 - l.L_DISCOUNT)) AS revenue
    FROM customer c
    JOIN orders o ON c.C_CUSTKEY = o.O_CUSTKEY
    JOIN lineitem l ON o.O_ORDERKEY = l.L_ORDERKEY
    JOIN nation n ON c.C_NATIONKEY = n.N_NATIONKEY
    JOIN region r ON n.N_REGIONKEY = r.R_REGIONKEY
    WHERE r.R_NAME = 'ASIA'
    AND o.O_ORDERDATE >= '1990-01-01'
    AND o.O_ORDERDATE <= '1994-12-31'
    GROUP BY n.N_NAME
    ORDER BY revenue DESC;
    """
    
    # Execute the query
    results = execute_query(mysql_connection, query)
    
    # Write results to CSV
    with open('query_output.csv', 'w', newline='') as csvfile:
        csvwriter = csv.writer(csvfile)
        # Write the header
        csvwriter.writerow(['N_NAME', 'REVENUE'])
        # Write the data
        for row in results:
            csvwriter.writerow(row)
    
    # Close the connection
    mysql_connection.close()

# Entry point
if __name__ == '__main__':
    main()
