import pymysql
import csv

# MySQL connection setup
mysql_connection = pymysql.connect(host='mysql',
                                   user='root',
                                   password='my-secret-pw',
                                   db='tpch')

# Function to execute a query in MySQL and return the results
def execute_mysql_query(connection, query):
    with connection.cursor() as cursor:
        cursor.execute(query)
        return cursor.fetchall()

# Calculate the average yearly loss revenue for parts with brand 'BRAND#23'
# and container 'MED BAG' with average quantity less than 20% of the average quantity
mysql_query = """
SELECT AVG(L_EXTENDEDPRICE / 7) AS avg_yearly_loss
FROM lineitem
JOIN part ON L_PARTKEY = P_PARTKEY
WHERE P_BRAND = 'BRAND#23' AND P_CONTAINER = 'MED BAG'
AND L_QUANTITY < (
    SELECT 0.2 * AVG(L_QUANTITY) FROM lineitem
    JOIN part ON L_PARTKEY = P_PARTKEY
    WHERE P_BRAND = 'BRAND#23' AND P_CONTAINER = 'MED BAG'
)
"""

# Execute the query on MySQL and get the result
mysql_result = execute_mysql_query(mysql_connection, mysql_query)

# Close the MySQL connection
mysql_connection.close()

# Output to file
with open('query_output.csv', 'w', newline='') as csvfile:
    writer = csv.writer(csvfile)
    writer.writerow(['avg_yearly_loss'])
    writer.writerows(mysql_result)
