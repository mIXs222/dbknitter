# query.py
import pymysql
import csv

# MySQL connection details
my_conn_data = {
    "host": "mysql",
    "user": "root",
    "password": "my-secret-pw",
    "database": "tpch"
}

# Open database connection
mysql_conn = pymysql.connect(**my_conn_data)
try:
    # Prepare a cursor object using the cursor() method
    cur = mysql_conn.cursor()
    
    # SQL query to execute
    query = """
    SELECT
        C_COUNT,
        COUNT(*) AS CUSTDIST
    FROM
        (
        SELECT
            C_CUSTKEY,
            COUNT(O_ORDERKEY) AS C_COUNT
        FROM
            customer LEFT OUTER JOIN orders ON
            C_CUSTKEY = O_CUSTKEY
            AND O_COMMENT NOT LIKE '%pending%deposits%'
        GROUP BY
            C_CUSTKEY
        ) AS C_ORDERS
    GROUP BY
        C_COUNT
    ORDER BY
        CUSTDIST DESC,
        C_COUNT DESC
    """
    
    # Execute the SQL query
    cur.execute(query)
    
    # Fetch all the rows
    results = cur.fetchall()
    
    # Write the output to a CSV file
    with open('query_output.csv', 'w', newline='') as csv_file:
        csv_writer = csv.writer(csv_file)
        csv_writer.writerow(['C_COUNT', 'CUSTDIST'])
        for row in results:
            csv_writer.writerow(row)
            
finally:
    # disconnect from server
    mysql_conn.close()
