# python_code.py
import pymongo
import pandas as pd
import direct_redis

# Connect to MongoDB
client = pymongo.MongoClient(host="mongodb", port=27017)
db = client['tpch']
orders_collection = db['orders']
# Query orders collection and convert to Pandas DataFrame
query = {
    'O_ORDERDATE': {'$gte': pd.Timestamp('1994-01-01'), '$lt': pd.Timestamp('1995-01-01')},
    'O_ORDERPRIORITY': {'$in': ['URGENT', 'HIGH']}
}
orders_df = pd.DataFrame(list(orders_collection.find(query, projection={'_id': False, 'O_ORDERKEY': True, 'O_ORDERPRIORITY': True})))

# Connect to Redis
redis_connection = direct_redis.DirectRedis(host="redis", port=6379, db=0)
# Get lineitem table from Redis and convert to Pandas DataFrame
lineitem_df = pd.read_json(redis_connection.get('lineitem'), orient='records')

# Perform analysis
filtered_lineitem_df = lineitem_df[
    (lineitem_df['L_SHIPDATE'] >= '1994-01-01') &
    (lineitem_df['L_SHIPDATE'] < '1995-01-01') &
    (lineitem_df['L_RECEIPTDATE'] > lineitem_df['L_COMMITDATE']) &
    (lineitem_df['L_SHIPMODE'].isin(['MAIL', 'SHIP']))
]

# Merge DataFrames on order key
joined_df = pd.merge(filtered_lineitem_df, orders_df, left_on='L_ORDERKEY', right_on='O_ORDERKEY', how='inner')

# Group by L_SHIPMODE and count the number of lineitems for each priority
result_df = joined_df.groupby(['L_SHIPMODE', 'O_ORDERPRIORITY']).size().reset_index(name='count')
result_df = result_df.pivot_table(index=['L_SHIPMODE'], columns='O_ORDERPRIORITY', values='count').reset_index()
result_df.columns.name = None
result_df = result_df.fillna(0).sort_values(by="L_SHIPMODE")

# Write the result to a CSV file
result_df.to_csv('query_output.csv', index=False)
