import pymysql
import csv

# Define MYSQL connection
def get_mysql_connection():
    return pymysql.connect(host='mysql', user='root', password='my-secret-pw', db='tpch')

# Function to execute a query and write to a CSV file
def execute_query_and_write_to_csv(query, csv_file_path):
    connection = get_mysql_connection()
    try:
        with connection.cursor() as cursor:
            cursor.execute(query)
            result = cursor.fetchall()
            headers = [i[0] for i in cursor.description]
            with open(csv_file_path, 'w', newline='', encoding='utf-8') as csv_file:
                csv_writer = csv.writer(csv_file)
                csv_writer.writerow(headers)
                for record in result:
                    csv_writer.writerow(record)
    finally:
        connection.close()

# Comprehensive analysis query
analysis_query = """
SELECT c.C_NAME, c.C_CUSTKEY, o.O_ORDERKEY, o.O_ORDERDATE, o.O_TOTALPRICE, SUM(l.L_QUANTITY) as total_quantity
FROM customer c
JOIN orders o ON c.C_CUSTKEY = o.O_CUSTKEY
JOIN lineitem l ON o.O_ORDERKEY = l.L_ORDERKEY
GROUP BY c.C_CUSTKEY, o.O_ORDERKEY
HAVING total_quantity > 300
ORDER BY o.O_TOTALPRICE DESC, o.O_ORDERDATE
"""

if __name__ == "__main__":
    csv_output_file = 'query_output.csv'
    execute_query_and_write_to_csv(analysis_query, csv_output_file)
