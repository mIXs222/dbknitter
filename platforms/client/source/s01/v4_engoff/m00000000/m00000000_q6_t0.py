import pymysql
import csv

# Connect to MySQL database
connection = pymysql.connect(host='mysql',
                             user='root',
                             password='my-secret-pw',
                             database='tpch')

try:
    with connection.cursor() as cursor:
        # SQL query to execute
        sql_query = """
        SELECT SUM(L_EXTENDEDPRICE * L_DISCOUNT) AS revenue_change
        FROM lineitem
        WHERE 
            L_SHIPDATE >= '1994-01-01' AND L_SHIPDATE < '1995-01-01' AND
            L_DISCOUNT BETWEEN 0.06 - 0.01 AND 0.06 + 0.01 AND
            L_QUANTITY < 24;
        """

        cursor.execute(sql_query)
        result = cursor.fetchone()
        
        # Writing output to csv
        with open('query_output.csv', 'w', newline='') as csvfile:
            fieldnames = ['revenue_change']
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerow({'revenue_change': result[0]})

finally:
    connection.close()
