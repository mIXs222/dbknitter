# query.py
import pymysql
import csv

# Connection details
connection_params = {
    'host': 'mysql',
    'user': 'root',
    'password': 'my-secret-pw',
    'database': 'tpch',
}

# SQL query
sql_query = """
SELECT SUM(L_EXTENDEDPRICE * L_DISCOUNT) AS REVENUE
FROM lineitem
WHERE 
  L_SHIPDATE > '1994-01-01' AND 
  L_SHIPDATE < '1995-01-01' AND
  L_DISCOUNT BETWEEN 0.06 - 0.01 AND 0.06 + 0.01 AND 
  L_QUANTITY < 24;
"""

# Connect to MySQL
connection = pymysql.connect(**connection_params)

try:
    with connection.cursor() as cursor:
        cursor.execute(sql_query)
        result = cursor.fetchall()
        
        # Write the query output to a csv file
        with open('query_output.csv', 'w', newline='') as file:
            csv_writer = csv.writer(file)
            csv_writer.writerow(['REVENUE'])  # header
            for row in result:
                csv_writer.writerow(row)

finally:
    connection.close()
