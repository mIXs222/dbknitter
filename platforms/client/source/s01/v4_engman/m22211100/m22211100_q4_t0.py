# query.py
import pymysql
import csv

# Function to connect to MySQL and execute query
def execute_mysql_query():
    connection = pymysql.connect(host='mysql',
                                 user='root',
                                 password='my-secret-pw',
                                 db='tpch',
                                 charset='utf8mb4')
    try:
        with connection.cursor() as cursor:
            query = """
                SELECT O_ORDERPRIORITY, COUNT(DISTINCT O_ORDERKEY) AS ORDER_COUNT
                FROM orders
                WHERE O_ORDERDATE >= '1993-07-01' AND O_ORDERDATE < '1993-10-01'
                AND EXISTS (
                    SELECT *
                    FROM lineitem
                    WHERE L_ORDERKEY = O_ORDERKEY AND L_COMMITDATE < L_RECEIPTDATE
                )
                GROUP BY O_ORDERPRIORITY
                ORDER BY O_ORDERPRIORITY ASC;
            """
            cursor.execute(query)
            results = cursor.fetchall()
            
            # Writing to query_output.csv
            with open('query_output.csv', mode='w', newline='') as csvfile:
                csv_writer = csv.writer(csvfile)
                csv_writer.writerow(['O_ORDERPRIORITY', 'ORDER_COUNT'])
                for row in results:
                    csv_writer.writerow(row)
    finally:
        connection.close()

# Execute the function
if __name__ == "__main__":
    execute_mysql_query()
