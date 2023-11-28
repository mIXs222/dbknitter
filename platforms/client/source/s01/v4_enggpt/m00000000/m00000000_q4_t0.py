import pymysql
import csv
from datetime import datetime

# MySQL connection
def mysql_connection():
    return pymysql.connect(host='mysql', user='root', passwd='my-secret-pw', db='tpch')

def write_to_csv(data):
    with open('query_output.csv', mode='w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(["Order Priority", "Count"]) # write header
        writer.writerows(data)

def fetch_data():
    connection = mysql_connection()
    try:
        with connection.cursor() as cursor:
            query = """
                SELECT o.O_ORDERPRIORITY, COUNT(DISTINCT o.O_ORDERKEY) as order_count
                FROM orders o
                JOIN lineitem l
                ON o.O_ORDERKEY = l.L_ORDERKEY
                WHERE o.O_ORDERDATE BETWEEN '1993-07-01' AND '1993-10-01'
                AND l.L_COMMITDATE < l.L_RECEIPTDATE
                GROUP BY o.O_ORDERPRIORITY
                ORDER BY o.O_ORDERPRIORITY ASC
            """
            cursor.execute(query)
            result = cursor.fetchall()
            return result
    finally:
        connection.close()

def main():
    result = fetch_data()
    write_to_csv(result)

if __name__ == "__main__":
    main()
