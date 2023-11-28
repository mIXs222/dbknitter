# query.py
import pymysql
import pymongo
import pandas as pd
from direct_redis import DirectRedis

# Establish a connection to MySQL
mysql_conn = pymysql.connect(
    host='mysql',
    user='root',
    password='my-secret-pw',
    db='tpch'
)

# Establish a connection to MongoDB
mongo_client = pymongo.MongoClient('mongodb', 27017)
mongo_db = mongo_client['tpch']

# Establish a connection to Redis
redis_conn = DirectRedis(host='redis', port=6379, db=0)

# Load the orders data from MySQL
query_orders = """
SELECT O_ORDERKEY, O_CUSTKEY, O_ORDERDATE, O_SHIPPRIORITY
FROM orders
WHERE O_ORDERDATE < '1995-03-15';
"""
orders_df = pd.read_sql(query_orders, mysql_conn)

# Load the lineitem data from MongoDB
lineitem_query = {
    'L_SHIPDATE': {'$gt': '1995-03-15'}
}
lineitem_projection = {
    '_id': False,
}
lineitem_df = pd.DataFrame(list(mongo_db.lineitem.find(lineitem_query, projection=lineitem_projection)))

# Load the customer data from Redis
customer_df = pd.read_json(redis_conn.get('customer'), orient='records')

# Merge the dataframes
merged_df = (
    customer_df[customer_df['C_MKTSEGMENT'] == 'BUILDING']
    .merge(orders_df, left_on='C_CUSTKEY', right_on='O_CUSTKEY')
    .merge(lineitem_df, left_on='O_ORDERKEY', right_on='L_ORDERKEY')
)

# Calculate the revenue
merged_df['revenue'] = merged_df['L_EXTENDEDPRICE'] * (1 - merged_df['L_DISCOUNT'])

# Group the merged dataframe
grouped_df = (
    merged_df
    .groupby(['O_ORDERKEY', 'O_ORDERDATE', 'O_SHIPPRIORITY'])
    .agg({'revenue': 'sum'})
    .reset_index()
)

# Sort the grouped dataframe
result_df = grouped_df.sort_values(by=['revenue', 'O_ORDERDATE'], ascending=[False, True])

# Write the result to a CSV file
result_df.to_csv('query_output.csv', index=False)

# Close the connections
mysql_conn.close()
mongo_client.close()
redis_conn.close()
