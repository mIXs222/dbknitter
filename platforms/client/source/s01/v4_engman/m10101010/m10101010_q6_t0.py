import pymysql
import csv

# Connect to the MySQL database
connection = pymysql.connect(host='mysql', user='root', password='my-secret-pw', database='tpch')
try:
    with connection.cursor() as cursor:
        # SQL Query
        sql = '''
        SELECT SUM(L_EXTENDEDPRICE * L_DISCOUNT) AS REVENUE
        FROM lineitem
        WHERE L_SHIPDATE > '1994-01-01'
          AND L_SHIPDATE < '1995-01-01'
          AND L_DISCOUNT BETWEEN (.06 - 0.01) AND (.06 + 0.01)
          AND L_QUANTITY < 24
        '''
        cursor.execute(sql)
        result = cursor.fetchone()

        # Write the output to a CSV file
        with open("query_output.csv", mode='w', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(['REVENUE'])  # Header
            writer.writerow(result)  # Query result row
finally:
    connection.close()
