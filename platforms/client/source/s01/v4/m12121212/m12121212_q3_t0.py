import pymongo
import pandas as pd
from direct_redis import DirectRedis
from datetime import datetime

# MongoDB connection
mongo_client = pymongo.MongoClient(host='mongodb', port=27017)
mongodb = mongo_client['tpch']
orders_collection = mongodb['orders']

# Querying the orders collection to get relevant orders
pipeline = [
    {"$match": {
        "O_ORDERDATE": {"$lt": datetime.strptime("1995-03-15", "%Y-%m-%d")},
        "O_ORDERSTATUS": {"$eq": "BUILDING"}  # Assuming order status represents market segment in the absence of customer data
    }},
    {"$project": {
        "_id": 0,
        "L_ORDERKEY": "$O_ORDERKEY",
        "O_ORDERDATE": "$O_ORDERDATE",
        "O_SHIPPRIORITY": "$O_SHIPPRIORITY"
    }}
]
orders_df = pd.DataFrame(list(orders_collection.aggregate(pipeline)))

# Reddis connection and query
redis_client = DirectRedis(host='redis', port=6379, db=0)

# Retrieve lineitem DataFrame from Redis
lineitem_df = pd.read_json(redis_client.get('lineitem'))
# Filter lineitem DataFrame as per the query condition
lineitem_df = lineitem_df[lineitem_df['L_SHIPDATE'] > '1995-03-15']

# Merge tables
result = pd.merge(orders_df, lineitem_df, how='inner', left_on='L_ORDERKEY', right_on='L_ORDERKEY')

# Compute 'REVENUE'
result['REVENUE'] = result['L_EXTENDEDPRICE'] * (1 - result['L_DISCOUNT'])

# Perform the final aggregation
final_result = (result.groupby(['L_ORDERKEY', 'O_ORDERDATE', 'O_SHIPPRIORITY'])
                .agg({'REVENUE': 'sum'})
                .reset_index()
                .sort_values(by=['REVENUE', 'O_ORDERDATE'], ascending=[False, True]))

# Select and rename columns as per the query output requirement
output = final_result[['L_ORDERKEY', 'O_ORDERDATE', 'O_SHIPPRIORITY', 'REVENUE']]
output.columns = ['L_ORDERKEY', 'O_ORDERDATE', 'O_SHIPPRIORITY', 'REVENUE']

# Write to CSV
output.to_csv('query_output.csv', index=False)
