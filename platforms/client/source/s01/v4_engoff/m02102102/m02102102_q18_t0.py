import pymysql
import pymongo
import pandas as pd
from direct_redis import DirectRedis

# Establish a connection to MySQL
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    database='tpch',
    charset='utf8mb4'
)

# Execute MySQL query to get orders with total quantity > 300
order_query = """
SELECT O_ORDERKEY, O_CUSTKEY, O_ORDERDATE, O_TOTALPRICE
FROM orders
WHERE O_TOTALPRICE > 300
"""
with mysql_conn.cursor() as cursor:
    cursor.execute(order_query)
    orders = cursor.fetchall()

# Transform orders into a DataFrame
orders_df = pd.DataFrame(orders, columns=['O_ORDERKEY', 'O_CUSTKEY', 'O_ORDERDATE', 'O_TOTALPRICE'])

# Close the MySQL connection
mysql_conn.close()

# Establish a connection to MongoDB
mongodb_client = pymongo.MongoClient(host='mongodb', port=27017)
mongodb_db = mongodb_client['tpch']
customer_collection = mongodb_db['customer']

# Get customers from MongoDB
customers = list(customer_collection.find({}, {'_id': 0, 'C_CUSTKEY': 1, 'C_NAME': 1}))
customers_df = pd.DataFrame(customers)

# Establish a connection to Redis
redis_client = DirectRedis(host='redis', port=6379, db=0)

# Get lineitem from Redis
lineitem = redis_client.get('lineitem')
lineitem_df = pd.read_msgpack(lineitem)

# Filter lineitem DataFrame for large quantity orders
large_orders = lineitem_df[lineitem_df['L_QUANTITY'] > 300]

# Merge DataFrames to get the final combined information
result = pd.merge(customers_df, orders_df, left_on='C_CUSTKEY', right_on='O_CUSTKEY')
result = pd.merge(result, large_orders[['L_ORDERKEY', 'L_QUANTITY']], left_on='O_ORDERKEY', right_on='L_ORDERKEY')
result = result[['C_NAME', 'C_CUSTKEY', 'O_ORDERKEY', 'O_ORDERDATE', 'O_TOTALPRICE', 'L_QUANTITY']]

# Write result to query_output.csv
result.to_csv('query_output.csv', index=False)
