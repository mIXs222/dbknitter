import pymysql
import csv

# Connect to the MySQL database
connection = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    database='tpch'
)

try:
    with connection.cursor() as cursor:
        # Execute the query on MySQL database
        query = """
        SELECT 
            c.C_NAME, c.C_CUSTKEY, o.O_ORDERKEY, o.O_ORDERDATE, o.O_TOTALPRICE, SUM(l.L_QUANTITY) AS total_quantity 
        FROM 
            customer AS c 
        JOIN 
            orders AS o ON c.C_CUSTKEY = o.O_CUSTKEY 
        JOIN 
            lineitem AS l ON o.O_ORDERKEY = l.L_ORDERKEY 
        GROUP BY 
            c.C_CUSTKEY, o.O_ORDERKEY 
        HAVING 
            SUM(l.L_QUANTITY) > 300
        """
        cursor.execute(query)
        results = cursor.fetchall()
        
        # Write the output to a CSV file
        with open('query_output.csv', 'w', newline='') as csvfile:
            csvwriter = csv.writer(csvfile)
            # Write the headers
            csvwriter.writerow(['C_NAME', 'C_CUSTKEY', 'O_ORDERKEY', 'O_ORDERDATE', 'O_TOTALPRICE', 'total_quantity'])
            # Write the data
            for row in results:
                csvwriter.writerow(row)
finally:
    # Close the database connection
    connection.close()
