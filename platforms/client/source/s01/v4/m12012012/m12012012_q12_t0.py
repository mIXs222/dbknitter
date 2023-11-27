import pymongo
import pandas as pd
from datetime import datetime

# MongoDB connection
client = pymongo.MongoClient('mongodb', 27017)
mongo_db = client['tpch']
orders_collection = mongo_db['orders']

# Retrieve orders data from MongoDB
orders_query = {
    "O_ORDERDATE": {
        "$gte": datetime(1994, 1, 1),
        "$lt": datetime(1995, 1, 1)
    }
}
orders_projection = {
    "O_ORDERKEY": 1,
    "O_ORDERPRIORITY": 1,
    "_id": 0
}
orders_df = pd.DataFrame(list(orders_collection.find(orders_query, orders_projection)))

# Redis connection
from direct_redis import DirectRedis
redis_client = DirectRedis(host='redis', port=6379, db=0)

# Retrieve lineitem data from Redis
lineitem_df = pd.read_json(redis_client.get('lineitem'), orient='records')

# Merge the dataframes for the join operation
merged_df = pd.merge(orders_df, lineitem_df, left_on='O_ORDERKEY', right_on='L_ORDERKEY')

# Filter based on the specified conditions
filtered_df = merged_df[(merged_df['L_SHIPMODE'].isin(['MAIL', 'SHIP'])) &
                        (merged_df['L_COMMITDATE'] < merged_df['L_RECEIPTDATE']) &
                        (merged_df['L_SHIPDATE'] < merged_df['L_COMMITDATE']) &
                        (merged_df['L_RECEIPTDATE'] >= datetime(1994, 1, 1)) &
                        (merged_df['L_RECEIPTDATE'] < datetime(1995, 1, 1))]

# Apply grouping and aggregation
grouped_df = filtered_df.groupby('L_SHIPMODE').agg(
    HIGH_LINE_COUNT=pd.NamedAgg(column='O_ORDERPRIORITY', aggfunc=lambda x: sum(x.isin(['1-URGENT', '2-HIGH']))),
    LOW_LINE_COUNT=pd.NamedAgg(column='O_ORDERPRIORITY', aggfunc=lambda x: sum(~x.isin(['1-URGENT', '2-HIGH']))),
).reset_index()

# Sort the results
sorted_df = grouped_df.sort_values('L_SHIPMODE')

# Write output to CSV
sorted_df.to_csv('query_output.csv', index=False)
