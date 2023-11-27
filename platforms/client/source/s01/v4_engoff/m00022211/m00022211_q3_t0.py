import pymongo
import redis
import pandas as pd
from datetime import datetime

# MongoDB connection
mongo_client = pymongo.MongoClient("mongodb", 27017)
mongo_db = mongo_client["tpch"]

# Redis connection (assuming direct_redis.DirectRedis is a modified class for a specific use case)
import direct_redis
redis_client = direct_redis.DirectRedis(host="redis", port=6379, db=0)
customer_data = redis_client.get('customer')
customers_df = pd.read_json(customer_data)

# Filter customers belonging to 'BUILDING' market segment
building_customers_df = customers_df[customers_df['C_MKTSEGMENT'] == 'BUILDING']

# Get orders from MongoDB
orders_coll = mongo_db['orders']
lineitem_coll = mongo_db['lineitem']

# Filter orders not shipped by 1995-03-15
query_date = datetime.strptime('1995-03-15', '%Y-%m-%d')
orders_not_shipped = list(orders_coll.find(
    {
        'O_ORDERDATE': {'$lt': query_date},
        'O_ORDERSTATUS': {'$nin': ['F', 'C']}    # Assuming 'F' and 'C' indicate finished or cancelled orders
    },
    {
        '_id': 0,
        'O_ORDERKEY': 1,
        'O_SHIPPRIORITY': 1
    }
))

# Convert to DataFrame
orders_df = pd.DataFrame(orders_not_shipped)

# Get lineitem data
pipeline = [
    {
        '$match': {
            'L_SHIPDATE': {'$gte': query_date}
        }
    },
    {
        '$group': {
            '_id': '$L_ORDERKEY',
            'revenue': {
                '$sum': {
                    '$multiply': [
                        '$L_EXTENDEDPRICE', 
                        {'$subtract': [1, '$L_DISCOUNT']}
                    ]
                }
            }
        }
    },
    {
        '$sort': {'revenue': -1}
    },
    {
        '$limit': 1
    }
]

top_revenue_order = list(lineitem_coll.aggregate(pipeline))

# Convert to DataFrame
top_revenue_df = pd.DataFrame(top_revenue_order).rename(columns={'_id': 'L_ORDERKEY', 'revenue': 'revenue'})

# Merge DataFrames to get final result
final_result = pd.merge(orders_df, top_revenue_df, how='inner', left_on='O_ORDERKEY', right_on='L_ORDERKEY')
final_result = pd.merge(final_result, building_customers_df, how='inner', left_on='O_CUSTKEY', right_on='C_CUSTKEY')

# Select required columns and write to CSV
output_columns = ['O_ORDERKEY', 'O_SHIPPRIORITY', 'revenue']
final_result.to_csv('query_output.csv', columns=output_columns, index=False)
