import pymysql
import csv

# Connection details
connection_details = {
    'host': 'mysql',
    'user': 'root',
    'password': 'my-secret-pw',
    'db': 'tpch',
    'charset': 'utf8mb4'
}

# Connect to MySQL
connection = pymysql.connect(**connection_details)

try:
    with connection.cursor() as cursor:
        # SQL query to execute
        sql = """
        SELECT o.O_ORDERPRIORITY, COUNT(DISTINCT o.O_ORDERKEY) AS order_count
        FROM orders o
        INNER JOIN lineitem l ON o.O_ORDERKEY = l.L_ORDERKEY
        WHERE o.O_ORDERDATE BETWEEN '1993-07-01' AND '1993-10-01'
        AND l.L_COMMITDATE < l.L_RECEIPTDATE
        GROUP BY o.O_ORDERPRIORITY
        ORDER BY o.O_ORDERPRIORITY ASC;
        """
        cursor.execute(sql)
        result = cursor.fetchall()

        # Write to CSV file
        with open('query_output.csv', 'w', newline='') as csvfile:
            fieldnames = ['O_ORDERPRIORITY', 'order_count']
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

            writer.writeheader()
            for row in result:
                writer.writerow({'O_ORDERPRIORITY': row[0], 'order_count': row[1]})

finally:
    connection.close()
