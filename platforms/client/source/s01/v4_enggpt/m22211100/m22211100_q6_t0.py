import pymysql
import csv

# Connect to the mysql database
connection = pymysql.connect(host='mysql',
                             user='root',
                             password='my-secret-pw',
                             database='tpch',
                             charset='utf8mb4',
                             cursorclass=pymysql.cursors.Cursor)

try:
    with connection.cursor() as cursor:
        query = """
        SELECT SUM(L_EXTENDEDPRICE * (1 - L_DISCOUNT)) AS total_revenue
        FROM lineitem
        WHERE L_SHIPDATE BETWEEN '1994-01-01' AND '1994-12-31'
        AND L_DISCOUNT BETWEEN 0.05 AND 0.07
        AND L_QUANTITY < 24
        """
        cursor.execute(query)
        result = cursor.fetchone()
        total_revenue = result[0] if result[0] is not None else 0

    # Write the output to a csv file
    with open('query_output.csv', mode='w') as output_file:
        csv_writer = csv.writer(output_file)
        csv_writer.writerow(['total_revenue'])
        csv_writer.writerow([total_revenue])
finally:
    connection.close()
