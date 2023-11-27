import pymysql
import csv

# Establish a connection to the MySQL database
connection = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    database='tpch'
)

try:
    with connection.cursor() as cursor:
        # Formulate the SQL query
        query = """
        SELECT
            o.O_ORDERKEY,
            o.O_SHIPPRIORITY,
            SUM(l.L_EXTENDEDPRICE * (1 - l.L_DISCOUNT)) AS revenue
        FROM
            orders o
            JOIN lineitem l ON o.O_ORDERKEY = l.L_ORDERKEY
            JOIN customer c ON o.O_CUSTKEY = c.C_CUSTKEY
        WHERE
            o.O_ORDERDATE < '1995-03-15'
            AND l.L_SHIPDATE > '1995-03-15'
            AND c.C_MKTSEGMENT = 'BUILDING'
        GROUP BY
            o.O_ORDERKEY,
            o.O_SHIPPRIORITY
        ORDER BY
            revenue DESC
        LIMIT 10;
        """

        # Execute the query
        cursor.execute(query)

        # Fetch the results
        results = cursor.fetchall()

        # Write results to CSV file
        with open('query_output.csv', mode='w', newline='') as file:
            writer = csv.writer(file)
            writer.writerow(['O_ORDERKEY', 'O_SHIPPRIORITY', 'REVENUE'])
            for row in results:
                writer.writerow(row)

finally:
    # Close the connection
    connection.close()
