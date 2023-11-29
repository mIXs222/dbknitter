import pymysql
import pymongo
import pandas as pd
from direct_redis import DirectRedis

# Connect to MySQL
mysql_connection = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    database='tpch'
)

# Connect to MongoDB
mongodb_client = pymongo.MongoClient(host='mongodb', port=27017)
mongodb_db = mongodb_client.tpch

# Connect to Redis
redis_client = DirectRedis(host='redis', port=6379, db=0)

# Fetch customers from MySQL
with mysql_connection.cursor() as cursor:
    cursor.execute("SELECT C_CUSTKEY, C_NAME FROM customer")
    customers = cursor.fetchall()
customers_df = pd.DataFrame(customers, columns=['C_CUSTKEY', 'C_NAME'])

# Fetch lineitems from MongoDB
lineitem_df = pd.DataFrame(list(mongodb_db.lineitem.find({})))

# Compute the sum of quantities for each order
lineitem_totals_df = lineitem_df.groupby('L_ORDERKEY').agg(
    total_quantity=pd.NamedAgg(column='L_QUANTITY', aggfunc='sum')
)

# Filter orders with total quantity > 300
large_volume_orders = lineitem_totals_df[lineitem_totals_df.total_quantity > 300]

# Fetch orders from Redis
orders_redis_df = pd.DataFrame(redis_client.get('orders'))  # Assuming DirectRedis works similar to redis-py

# Merge to get large volume orders
large_orders_df = orders_redis_df.merge(large_volume_orders, left_on='O_ORDERKEY', right_index=True)

# Join with customer information
large_volume_customer_orders = customers_df.merge(large_orders_df, left_on='C_CUSTKEY', right_on='O_CUSTKEY')

# Select the required columns
result_df = large_volume_customer_orders[[
    'C_NAME', 'C_CUSTKEY', 'O_ORDERKEY', 'O_ORDERDATE', 'O_TOTALPRICE', 'total_quantity'
]]

# Sort the results as requested
sorted_result_df = result_df.sort_values(by=['O_TOTALPRICE', 'O_ORDERDATE'], ascending=[False, True])

# Write to csv file
sorted_result_df.to_csv('query_output.csv', index=False)

# Close all database connections
mysql_connection.close()
mongodb_client.close()
redis_client.close()
