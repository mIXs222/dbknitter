# mysql_query.py
import pymysql
import csv

# Connection details
mysql_config = {
    'host': 'mysql',
    'user': 'root',
    'password': 'my-secret-pw',
    'db': 'tpch',
}

# Connect to MySQL
connection = pymysql.connect(**mysql_config)
try:
    with connection.cursor() as cursor:
        # SQL query
        query = """
        SELECT O_ORDERPRIORITY, COUNT(DISTINCT O_ORDERKEY) as order_count
        FROM orders
        INNER JOIN lineitem ON O_ORDERKEY = L_ORDERKEY
        WHERE O_ORDERDATE BETWEEN '1993-07-01' AND '1993-10-01'
        AND L_COMMITDATE < L_RECEIPTDATE
        GROUP BY O_ORDERPRIORITY
        ORDER BY O_ORDERPRIORITY ASC;
        """
        cursor.execute(query)

        # Fetch the results
        results = cursor.fetchall()

        # Write results to CSV file
        with open('query_output.csv', mode='w', newline='') as csvfile:
            csv_writer = csv.writer(csvfile)
            csv_writer.writerow(['O_ORDERPRIORITY', 'order_count'])
            for row in results:
                csv_writer.writerow(row)

finally:
    connection.close()
