from pymongo import MongoClient
import direct_redis
import pandas as pd

# MongoDB connection and query
mongo_client = MongoClient('mongodb', 27017)
mongo_db = mongo_client['tpch']
lineitem_collection = mongo_db['lineitem']

lineitem_query = {
    'L_SHIPMODE': {'$in': ['MAIL', 'SHIP']},
    'L_COMMITDATE': {'$lt': 'L_RECEIPTDATE'},
    'L_SHIPDATE': {'$lt': 'L_COMMITDATE'},
    'L_RECEIPTDATE': {'$gte': '1994-01-01', '$lt': '1995-01-01'}
}
lineitem_projection = {'L_ORDERKEY': 1, 'L_SHIPMODE': 1}

lineitem_df = pd.DataFrame(list(lineitem_collection.find(lineitem_query, lineitem_projection)))

# Redis connection and data retrieval
redis_connection = direct_redis.DirectRedis(host='redis', port=6379, db=0)
orders_df = pd.read_json(redis_connection.get('orders'))

# Merge and process data
merged_df = pd.merge(orders_df, lineitem_df, how='inner', left_on='O_ORDERKEY', right_on='L_ORDERKEY')

# Aggregate
result_df = merged_df.groupby('L_SHIPMODE').agg(
    HIGH_LINE_COUNT=('O_ORDERPRIORITY', lambda x: ((x == '1-URGENT') | (x == '2-HIGH')).sum()),
    LOW_LINE_COUNT=('O_ORDERPRIORITY', lambda x: ((x != '1-URGENT') & (x != '2-HIGH')).sum())
).reset_index()

# Write to CSV
result_df.to_csv('query_output.csv', index=False)
