# mysql_query.py
import pymysql
import csv

# Connection information for MySQL
mysql_conn_info = {
    'host': 'mysql',
    'user': 'root',
    'password': 'my-secret-pw',
    'db': 'tpch',
    'charset': 'utf8mb4',
    'cursorclass': pymysql.cursors.Cursor
}

# Attempt to connect to the mysql database
try:
    mysql_conn = pymysql.connect(**mysql_conn_info)
    print("Connected to MySQL")
except Exception as e:
    print(f"Error connecting to MySQL: {e}")

try:
    with mysql_conn.cursor() as cursor:
        # Define the MySQL query
        query = """
        SELECT C_COUNT, COUNT(*) AS CUSTDIST
        FROM (
            SELECT C.C_CUSTKEY, COUNT(O.O_ORDERKEY) AS C_COUNT
            FROM customer C
            LEFT JOIN orders O ON C.C_CUSTKEY = O.O_CUSTKEY
            AND O.O_COMMENT NOT LIKE '%pending%' AND O.O_COMMENT NOT LIKE '%deposits%'
            GROUP BY C.C_CUSTKEY
        ) AS C_ORDERS
        GROUP BY C_COUNT
        ORDER BY CUSTDIST DESC, C_COUNT DESC
        """

        # Execute the query
        cursor.execute(query)
        results = cursor.fetchall()

        # Write output to csv file
        with open('query_output.csv', 'w', newline='') as csvfile:
            csvwriter = csv.writer(csvfile)
            # Write header
            csvwriter.writerow(['C_COUNT', 'CUSTDIST'])
            for row in results:
                csvwriter.writerow(row)
        print("Query results written to query_output.csv")

except Exception as e:
    print(f"Error running query on MySQL: {e}")
finally:
    if mysql_conn:
        mysql_conn.close()
