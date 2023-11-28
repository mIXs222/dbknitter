import pymysql
import csv

# Establish a connection to the MySQL database
connection = pymysql.connect(host='mysql',
                             user='root',
                             password='my-secret-pw',
                             db='tpch')
try:
    with connection.cursor() as cursor:
        # SQL query string
        sql_query = '''
        SELECT SUM(L_EXTENDEDPRICE * (1 - L_DISCOUNT)) AS total_revenue
        FROM lineitem
        WHERE L_SHIPDATE BETWEEN '1994-01-01' AND '1994-12-31'
          AND L_DISCOUNT BETWEEN 0.05 AND 0.07
          AND L_QUANTITY < 24;
        '''
        # Execute the query
        cursor.execute(sql_query)
        # Fetch the result
        result = cursor.fetchone()
        
        # Write result to file
        with open('query_output.csv', 'w', newline='') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(['total_revenue'])
            writer.writerow(result)

finally:
    # Close the connection
    connection.close()
