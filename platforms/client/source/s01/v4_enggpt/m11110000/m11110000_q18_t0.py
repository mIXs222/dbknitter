import pymysql
import csv

# Define the connection details for the MySQL database
mysql_connection = {
    'host': 'mysql',
    'user': 'root',
    'password': 'my-secret-pw',
    'db': 'tpch',
    'charset': 'utf8mb4',
    'cursorclass': pymysql.cursors.Cursor
}

# Connect to the MySQL database
conn = pymysql.connect(**mysql_connection)

try:
    with conn.cursor() as cursor:
        # SQL query
        sql = """
        SELECT c.C_NAME, c.C_CUSTKEY, o.O_ORDERKEY, o.O_ORDERDATE, o.O_TOTALPRICE, SUM(l.L_QUANTITY) as total_quantity
        FROM customer c
        JOIN orders o ON c.C_CUSTKEY = o.O_CUSTKEY
        JOIN lineitem l ON o.O_ORDERKEY = l.L_ORDERKEY
        WHERE o.O_ORDERKEY IN (
            SELECT l2.L_ORDERKEY
            FROM lineitem l2
            GROUP BY l2.L_ORDERKEY
            HAVING SUM(l2.L_QUANTITY) > 300
        )
        GROUP BY c.C_NAME, c.C_CUSTKEY, o.O_ORDERKEY, o.O_ORDERDATE, o.O_TOTALPRICE
        ORDER BY o.O_TOTALPRICE DESC, o.O_ORDERDATE DESC
        """
        # Execute the SQL query
        cursor.execute(sql)
        result = cursor.fetchall()

        # Write the result to the file
        with open('query_output.csv', 'w', newline='', encoding='utf-8') as csvfile:
            csvwriter = csv.writer(csvfile)
            # Write header
            csvwriter.writerow(['C_NAME', 'C_CUSTKEY', 'O_ORDERKEY', 'O_ORDERDATE', 'O_TOTALPRICE', 'TOTAL_QUANTITY'])
            # Write the data
            for row in result:
                csvwriter.writerow(row)

finally:
    conn.close()
