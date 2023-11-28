import pymysql
import pandas as pd
from direct_redis import DirectRedis

# Function to get a connection to MySQL database
def get_mysql_connection(db_name, user, password, host):
    return pymysql.connect(host=host, user=user, passwd=password, db=db_name)

# Function to execute a query on MySQL and return the result as a Pandas DataFrame
def query_mysql(connection, query):
    return pd.read_sql(query, connection)

# Establish connection to MySQL
mysql_connection = get_mysql_connection(db_name='tpch', user='root', password='my-secret-pw', host='mysql')

# Subquery to get orders with total quantity > 300
subquery = """
SELECT L_ORDERKEY
FROM lineitem
GROUP BY L_ORDERKEY
HAVING SUM(L_QUANTITY) > 300
"""

# Main query to get orders and join with customer information
main_query = """
SELECT c.C_NAME, o.O_CUSTKEY, o.O_ORDERKEY, o.O_ORDERDATE, o.O_TOTALPRICE, SUM(l.L_QUANTITY) as TOTAL_QUANTITY
FROM ({}) as sub
JOIN orders as o ON sub.L_ORDERKEY = o.O_ORDERKEY
JOIN lineitem as l ON l.L_ORDERKEY = o.O_ORDERKEY
GROUP BY c.C_NAME, o.O_CUSTKEY, o.O_ORDERKEY, o.O_ORDERDATE, o.O_TOTALPRICE
ORDER BY o.O_TOTALPRICE DESC, o.O_ORDERDATE
""".format(subquery)

# Perform the query on MySQL
order_data = query_mysql(mysql_connection, main_query)

# Close MySQL connection
mysql_connection.close()

# Connect to Redis using DirectRedis
redis_connection = DirectRedis(host='redis', port=6379, db=0)

# Get the customer DataFrame from Redis
customer_data = pd.DataFrame(redis_connection.get('customer'))

# Merge the customer information with order_data obtained from MySQL
result_data = pd.merge(customer_data, order_data, left_on='C_CUSTKEY', right_on='O_CUSTKEY')

# Write the result to CSV file
result_data.to_csv('query_output.csv', index=False)
