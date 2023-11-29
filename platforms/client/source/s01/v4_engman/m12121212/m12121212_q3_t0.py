# import necessary libraries
import pymongo
from pymongo import MongoClient
import direct_redis
import pandas as pd
from datetime import datetime

# Connect to MongoDB
mongo_client = MongoClient('mongodb', 27017)
mongo_db = mongo_client['tpch']

# Query MongoDB for orders meeting the date criteria and is BUILDING segment
orders_pipeline = [
    {
        '$lookup': {
            'from': 'customer',
            'localField': 'O_CUSTKEY',
            'foreignField': 'C_CUSTKEY',
            'as': 'customer_info'
        }
    },
    {'$unwind': '$customer_info'},
    {
        '$match': {
            'O_ORDERDATE': {'$lt': datetime(1995, 3, 5)},
            'customer_info.C_MKTSEGMENT': 'BUILDING'
        }
    },
    {
        '$project': {
            '_id': 0,
            'O_ORDERKEY': 1,
            'O_ORDERDATE': 1,
            'O_SHIPPRIORITY': 1            
        }
    }
]
orders_df = pd.DataFrame(list(mongo_db['orders'].aggregate(orders_pipeline)))

# Connect to Redis
redis_client = direct_redis.DirectRedis(host='redis', port=6379, db=0)

# Get lineitem table from Redis
lineitem_df_data = redis_client.get('lineitem')
lineitem_df = pd.read_json(lineitem_df_data)

# Filter lineitem records based on shipdate
lineitem_df = lineitem_df[lineitem_df['L_SHIPDATE'] > datetime(1995, 3, 15)]

# Calculate revenue for lineitem
lineitem_df['REVENUE'] = lineitem_df['L_EXTENDEDPRICE'] * (1 - lineitem_df['L_DISCOUNT'])

# Merge orders and lineitem on O_ORDERKEY and L_ORDERKEY, and compute total revenue
result_df = orders_df.merge(lineitem_df, left_on='O_ORDERKEY', right_on='L_ORDERKEY')
result_df['REVENUE'] = result_df.groupby('O_ORDERKEY')['REVENUE'].transform('sum')
result_df = result_df.drop_duplicates('O_ORDERKEY')

# Filter the required columns and sort by REVENUE in descending order
output_df = result_df[['O_ORDERKEY', 'REVENUE', 'O_ORDERDATE', 'O_SHIPPRIORITY']]
output_df = output_df.sort_values('REVENUE', ascending=False)
output_df.to_csv('query_output.csv', index=False)
