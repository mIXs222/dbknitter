# query.py
import pymysql
import csv

# Establish connection to MySQL database
connection = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    db='tpch',
    charset='utf8mb4'
)

try:
    with connection.cursor() as cursor:
        # Define the query
        sql_query = """
            SELECT SUM(L_EXTENDEDPRICE * (1 - L_DISCOUNT)) AS total_revenue
            FROM lineitem 
            WHERE L_SHIPDATE BETWEEN '1994-01-01' AND '1994-12-31'
            AND L_DISCOUNT BETWEEN 0.05 AND 0.07
            AND L_QUANTITY < 24;
        """

        # Execute the query
        cursor.execute(sql_query)
        
        # Fetch the result
        result = cursor.fetchone()

        # Prepare data to be written to CSV
        with open('query_output.csv', mode='w', newline='', encoding='utf-8') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(['total_revenue'])
            writer.writerow([result[0]])

finally:
    # Close connection
    connection.close()
