import pymysql
import csv

# Function to connect to MySQL
def connect_to_mysql():
    return pymysql.connect(
        host='mysql',
        user='root',
        password='my-secret-pw',
        db='tpch',
        charset='utf8mb4'
    )

# Function to execute a query on MySQL
def execute_mysql_query(connection, query):
    with connection.cursor() as cursor:
        cursor.execute(query)
        return cursor.fetchall()

# Function to write the results to a CSV file
def write_to_csv(data, file_name):
    headers = ["C_COUNT", "CUSTDIST"]
    with open(file_name, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(headers)
        writer.writerows(data)

# SQL query
mysql_query = """
    SELECT
        C_COUNT,
        COUNT(*) AS CUSTDIST
    FROM (
        SELECT
            C_CUSTKEY,
            COUNT(O_ORDERKEY) AS C_COUNT
        FROM
            customer AS c
            LEFT JOIN orders AS o ON c.C_CUSTKEY = o.O_CUSTKEY
            AND o.O_COMMENT NOT LIKE '%pending%'
            AND o.O_COMMENT NOT LIKE '%deposits%'
        GROUP BY
            C_CUSTKEY
    ) AS C_ORDERS
    GROUP BY
        C_COUNT
    ORDER BY
        CUSTDIST DESC,
        C_COUNT DESC;
"""

# Main Execution
if __name__ == '__main__':
    # Connect to the MySQL database
    mysql_conn = connect_to_mysql()
    
    try:
        # Execute the query on MySQL and fetch the results
        mysql_results = execute_mysql_query(mysql_conn, mysql_query)
        
        # Write the results to a CSV file
        write_to_csv(mysql_results, 'query_output.csv')
    finally:
        mysql_conn.close()
