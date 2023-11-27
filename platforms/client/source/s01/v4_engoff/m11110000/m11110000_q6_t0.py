import pymysql
import csv

# MySQL connection details
mysql_conn_info = {
    'host': 'mysql',
    'user': 'root',
    'password': 'my-secret-pw',
    'db': 'tpch',
}

# Connect to MySQL
mysql_conn = pymysql.connect(**mysql_conn_info)
try:
    with mysql_conn.cursor() as cursor:
        # Write the SQL query
        sql = '''
        SELECT SUM(L_EXTENDEDPRICE * L_DISCOUNT) AS revenue_change
        FROM lineitem
        WHERE 
            L_SHIPDATE >= '1994-01-01' AND 
            L_SHIPDATE < '1995-01-01' AND 
            L_DISCOUNT BETWEEN .06 - 0.01 AND .06 + 0.01 AND 
            L_QUANTITY < 24
        '''
        # Execute the query
        cursor.execute(sql)
        
        # Fetch the result
        result = cursor.fetchone()
        revenue_change = result[0] if result[0] is not None else 0
finally:
    mysql_conn.close()

# Write output to csv
with open('query_output.csv', mode='w', newline='') as file:
    writer = csv.writer(file)
    writer.writerow(['revenue_change']) # header row
    writer.writerow([revenue_change])
