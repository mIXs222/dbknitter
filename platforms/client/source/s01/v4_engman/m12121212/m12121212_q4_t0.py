import pymongo
import direct_redis
import pandas as pd
from datetime import datetime

# Connect to MongoDB
mongo_client = pymongo.MongoClient("mongodb", 27017)
mongo_db = mongo_client['tpch']
orders_collection = mongo_db['orders']

# Connect to Redis
redis_client = direct_redis.DirectRedis(host='redis', port=6379, db=0)

# Define the date range for the query
start_date = datetime(1993, 7, 1)
end_date = datetime(1993, 10, 1)

# Fetch orders from MongoDB
orders_query = {
    'O_ORDERDATE': {
        '$gte': start_date,
        '$lt': end_date
    }
}
orders_df = pd.DataFrame(list(orders_collection.find(orders_query, projection={'_id': False})))

# Fetch lineitems from Redis stored as pandas DataFrame
lineitems_df = redis_client.get('lineitem')
if isinstance(lineitems_df, bytes):
    lineitems_df = pd.read_msgpack(lineitems_df)

# Join orders with lineitems on O_ORDERKEY == L_ORDERKEY
joined_df = orders_df.merge(lineitems_df, left_on='O_ORDERKEY', right_on='L_ORDERKEY')

# Filter lineitems where receipt is later than commitment
late_receipt_df = joined_df[joined_df['L_RECEIPTDATE'] > joined_df['L_COMMITDATE']]

# Count the number of distinct orders for each O_ORDERPRIORITY that have a late receipt
priority_count = (
    late_receipt_df.groupby('O_ORDERPRIORITY')['O_ORDERKEY']
    .nunique()
    .reset_index(name='ORDER_COUNT')
    .sort_values(by='O_ORDERPRIORITY')
)

# Save to CSV file
priority_count.to_csv('query_output.csv', index=False)
