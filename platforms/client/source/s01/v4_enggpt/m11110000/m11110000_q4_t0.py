import pymysql
import csv

# MySQL connection
def get_mysql_data():
    connection = pymysql.connect(host='mysql',
                                 user='root',
                                 password='my-secret-pw',
                                 db='tpch')
    try:
        with connection.cursor() as cursor:
            query = """
                SELECT O_ORDERPRIORITY, COUNT(DISTINCT O_ORDERKEY) AS order_count
                FROM orders
                WHERE O_ORDERDATE >= '1993-07-01' 
                  AND O_ORDERDATE < '1993-10-01'
                  AND EXISTS (
                    SELECT * 
                    FROM lineitem 
                    WHERE L_ORDERKEY = O_ORDERKEY 
                      AND L_COMMITDATE < L_RECEIPTDATE
                  )
                GROUP BY O_ORDERPRIORITY
                ORDER BY O_ORDERPRIORITY ASC;
            """
            cursor.execute(query)
            result = cursor.fetchall()
            return result
    finally:
        connection.close()

# Fetch the data from the databases
mysql_data = get_mysql_data()

# Write the query result to a CSV file
with open('query_output.csv', 'w', newline='') as csvfile:
    csv_writer = csv.writer(csvfile)
    csv_writer.writerow(['O_ORDERPRIORITY', 'order_count']) # header row
    for row in mysql_data:
        csv_writer.writerow(row)
