from pymongo import MongoClient
from direct_redis import DirectRedis
import pandas as pd
from datetime import datetime

# Connect to MongoDB
mongo_client = MongoClient(host="mongodb", port=27017)
mongo_db = mongo_client["tpch"]
orders_collection = mongo_db["orders"]

# Connect to Redis
redis = DirectRedis(host="redis", port=6379, db=0)

# Fetch data from MongoDB
orders_query = {
    "O_ORDERDATE": {
        "$gte": datetime(1994, 1, 1),
        "$lte": datetime(1994, 12, 31)
    },
    "O_ORDERPRIORITY": {
        "$in": ["1-URGENT", "2-HIGH"]
    }
}
orders_df = pd.DataFrame(list(orders_collection.find(orders_query, projection={"_id": 0})))
orders_df.set_index('O_ORDERKEY', inplace=True)

# Fetch data from Redis
lineitems_raw = redis.get('lineitem')
lineitems_df = pd.read_json(lineitems_raw, orient='records')
lineitems_df.set_index('L_ORDERKEY', inplace=True)

# Merge data
merged_df = lineitems_df.join(orders_df, how="inner", rsuffix='_order')

# Filter based on criteria and calculate counts
filtered_df = merged_df[
    (merged_df['L_SHIPMODE'].isin(['MAIL', 'SHIP'])) &
    (filtered_df['L_COMMITDATE'] < filtered_df['L_RECEIPTDATE']) &
    (filtered_df['L_SHIPDATE'] < filtered_df['L_COMMITDATE'])
]

# Count line items for high and low priority
shipping_modes = filtered_df['L_SHIPMODE'].unique()
results = []
for mode in shipping_modes:
    high_priority_count = filtered_df[
        (filtered_df['L_SHIPMODE'] == mode) &
        (filtered_df['O_ORDERPRIORITY'].isin(['1-URGENT', '2-HIGH']))
    ].shape[0]
    low_priority_count = filtered_df[
        (filtered_df['L_SHIPMODE'] == mode) &
        (~filtered_df['O_ORDERPRIORITY'].isin(['1-URGENT', '2-HIGH']))
    ].shape[0]
    results.append({'SHIPPING_MODE': mode, 'HIGH_LINE_COUNT': high_priority_count, 'LOW_LINE_COUNT': low_priority_count})

# Create the results DataFrame
results_df = pd.DataFrame(results)
results_df.sort_values('SHIPPING_MODE', ascending=True, inplace=True)

# Write to CSV
results_df.to_csv('query_output.csv', index=False)
