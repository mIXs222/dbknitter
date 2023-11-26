import pymongo
import pandas as pd
from direct_redis import DirectRedis
from datetime import datetime

# Connect to MongoDB
mongo_client = pymongo.MongoClient("mongodb", 27017)
mongo_db = mongo_client['tpch']
orders_col = mongo_db['orders']

# Connect to Redis
redis_client = DirectRedis(host='redis', port=6379, db=0)

# Query orders from MongoDB
orders_query = {
    "O_ORDERDATE": {
        "$gte": datetime(1994, 1, 1),
        "$lt": datetime(1995, 1, 1)
    }
}
orders_df = pd.DataFrame(list(orders_col.find(orders_query)))

# Query lineitem from Redis
lineitem_df = pd.DataFrame(redis_client.get('lineitem'))

# Convert dates for lineitem DataFrame
lineitem_df['L_SHIPDATE'] = pd.to_datetime(lineitem_df['L_SHIPDATE'])
lineitem_df['L_COMMITDATE'] = pd.to_datetime(lineitem_df['L_COMMITDATE'])
lineitem_df['L_RECEIPTDATE'] = pd.to_datetime(lineitem_df['L_RECEIPTDATE'])

# Clean and transform DataFrame from string literals
lineitem_df = lineitem_df.apply(pd.to_numeric, errors='ignore')

# Filter and join on the O_ORDERKEY
lineitem_df = lineitem_df[
    (lineitem_df['L_SHIPMODE'].isin(['MAIL', 'SHIP'])) &
    (lineitem_df['L_COMMITDATE'] < lineitem_df['L_RECEIPTDATE']) &
    (lineitem_df['L_SHIPDATE'] < lineitem_df['L_COMMITDATE']) &
    (lineitem_df['L_RECEIPTDATE'] >= pd.Timestamp('1994-01-01')) &
    (lineitem_df['L_RECEIPTDATE'] < pd.Timestamp('1995-01-01'))
]

merged_df = pd.merge(orders_df, lineitem_df, left_on='O_ORDERKEY', right_on='L_ORDERKEY')

# Perform analysis
result_df = merged_df.groupby('L_SHIPMODE').apply(
    lambda x: pd.Series({
        'HIGH_LINE_COUNT': ((x['O_ORDERPRIORITY'] == '1-URGENT') | (x['O_ORDERPRIORITY'] == '2-HIGH')).sum(),
        'LOW_LINE_COUNT': (~(x['O_ORDERPRIORITY'].isin(['1-URGENT', '2-HIGH']))).sum()
    })
).reset_index()

# Write to CSV
result_df.to_csv('query_output.csv', index=False)
