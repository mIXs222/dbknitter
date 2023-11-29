import pymysql
import pymongo
import pandas as pd
import direct_redis
from sqlalchemy import create_engine

# Connect to MySQL
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    database='tpch'
)

# Connect to MongoDB
mongo_client = pymongo.MongoClient(host='mongodb', port=27017)
mongodb = mongo_client['tpch']

# Connect to Redis with direct_redis
redis_conn = direct_redis.DirectRedis(host='redis', port=6379, db=0)

# Query MySQL for orders with total quantity larger than 300
with mysql_conn.cursor() as cursor:
    cursor.execute("""
        SELECT O_CUSTKEY, O_ORDERKEY, O_ORDERDATE, O_TOTALPRICE
        FROM orders
        WHERE O_TOTALPRICE > 300
    """)
    orders_df = pd.DataFrame(cursor.fetchall(), columns=['O_CUSTKEY', 'O_ORDERKEY', 'O_ORDERDATE', 'O_TOTALPRICE'])

# Query MongoDB for customer information
customers_df = pd.DataFrame(mongodb.customer.find({}, {'C_CUSTKEY': 1, 'C_NAME': 1}))

# Unpickle lineitem DataFrame from Redis
lineitem_df = pd.DataFrame(redis_conn.get('lineitem'))

# Merge DataFrames to get the final result
result_df = pd.merge(customers_df, orders_df, left_on='C_CUSTKEY', right_on='O_CUSTKEY')
result_df = pd.merge(result_df, lineitem_df, left_on='O_ORDERKEY', right_on='L_ORDERKEY')

# Make sure to handle the quantity as numeric and filter for large orders
result_df['L_QUANTITY'] = pd.to_numeric(result_df['L_QUANTITY'])
large_orders_df = result_df[result_df['L_QUANTITY'] > 300]

# Select necessary columns for output
final_df = large_orders_df[['C_NAME', 'C_CUSTKEY', 'O_ORDERKEY', 'O_ORDERDATE', 'O_TOTALPRICE', 'L_QUANTITY']]

# Sort the results as required by the query
final_df = final_df.sort_values(by=['O_TOTALPRICE', 'O_ORDERDATE'], ascending=[False, True])

# Write to CSV file
final_df.to_csv('query_output.csv', index=False)

# Close connections
mysql_conn.close()
mongo_client.close()
redis_conn.close()
