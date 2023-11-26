from pymongo import MongoClient
from direct_redis import DirectRedis
import pandas as pd
from datetime import datetime

# Connect to MongoDB
mongo_client = MongoClient('mongodb', 27017)
mongo_db = mongo_client['tpch']
orders_collection = mongo_db['orders']

# Query orders from MongoDB
query = {
    'O_ORDERDATE': {'$gte': datetime(1993, 7, 1), '$lt': datetime(1993, 10, 1)}
}
orders_df = pd.DataFrame(list(orders_collection.find(query, projection={'_id': False})))

# Connect to Redis
redis_client = DirectRedis(host='redis', port=6379, db=0)

# Get lineitem data from Redis and convert to Pandas DataFrame
lineitem_df = pd.read_json(redis_client.get('lineitem'), orient='records')
lineitem_df['L_COMMITDATE'] = pd.to_datetime(lineitem_df['L_COMMITDATE'])
lineitem_df['L_RECEIPTDATE'] = pd.to_datetime(lineitem_df['L_RECEIPTDATE'])

# Filter lineitem to find rows where L_COMMITDATE < L_RECEIPTDATE
filtered_lineitem_df = lineitem_df[lineitem_df['L_COMMITDATE'] < lineitem_df['L_RECEIPTDATE']]

# Merge orders and filtered lineitem on L_ORDERKEY to check for existence 
result_df = pd.merge(
    orders_df,
    filtered_lineitem_df[['L_ORDERKEY']],
    how='inner',
    left_on='O_ORDERKEY',
    right_on='L_ORDERKEY'
)

# Group the resulting DataFrame by O_ORDERPRIORITY and count the orders
output_df = result_df.groupby('O_ORDERPRIORITY') \
                     .size() \
                     .reset_index(name='ORDER_COUNT')

# Order the result by O_ORDERPRIORITY
output_df = output_df.sort_values('O_ORDERPRIORITY')

# Write the output to a csv file
output_df.to_csv('query_output.csv', index=False)
