import pymysql
import csv

# Connect to the MySQL server
connection = pymysql.connect(host='mysql',
                             user='root',
                             password='my-secret-pw',
                             database='tpch',
                             charset='utf8mb4',
                             cursorclass=pymysql.cursors.Cursor)

try:
    with connection.cursor() as cursor:
        # Execute the required SQL query
        query = """
        SELECT C_COUNT, COUNT(*) AS CUSTDIST
        FROM (
            SELECT c.C_CUSTKEY, COUNT(o.O_ORDERKEY) AS C_COUNT
            FROM customer c
            LEFT OUTER JOIN orders o ON c.C_CUSTKEY = o.O_CUSTKEY
            AND o.O_COMMENT NOT LIKE '%pending%' AND o.O_COMMENT NOT LIKE '%deposits%'
            GROUP BY c.C_CUSTKEY
        ) AS C_ORDERS
        GROUP BY C_COUNT
        ORDER BY CUSTDIST DESC, C_COUNT DESC;
        """
        cursor.execute(query)
        results = cursor.fetchall()

        # Write the results to a CSV file
        with open('query_output.csv', 'w', newline='') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(['C_COUNT', 'CUSTDIST'])  # column headers
            for row in results:
                writer.writerow(row)

finally:
    connection.close()
