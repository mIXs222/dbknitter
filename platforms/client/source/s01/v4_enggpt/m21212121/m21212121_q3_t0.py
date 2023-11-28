import pymongo
import pandas as pd
from bson.son import SON  # To ensure order when doing group by
from datetime import datetime
import direct_redis

# MongoDB connection
mongo_client = pymongo.MongoClient('mongodb', 27017)
mongo_db = mongo_client['tpch']
customer_col = mongo_db['customer']
lineitem_col = mongo_db['lineitem']

# Redis connection
redis_client = direct_redis.DirectRedis(host='redis', port=6379, db=0)

# MongoDB query for customers in the 'BUILDING' market segment
building_customers = customer_col.find({'C_MKTSEGMENT': 'BUILDING'}, {'_id': 0, 'C_CUSTKEY': 1})

building_cust_keys = [cust['C_CUSTKEY'] for cust in building_customers]

# Redis query for orders with order date before March 15, 1995
orders_df = redis_client.get_df('orders')
orders_df['O_ORDERDATE'] = pd.to_datetime(orders_df['O_ORDERDATE'])
orders_before_mar15 = orders_df[(orders_df['O_ORDERDATE'] < '1995-03-15') & (orders_df['O_CUSTKEY'].isin(building_cust_keys))]

# MongoDB query for line items with ship date after March 15, 1995
march_15_1995 = datetime(1995, 3, 15)
pipeline = [
    {
        '$match': {
            'L_SHIPDATE': {'$gt': march_15_1995},
            'L_ORDERKEY': {'$in': orders_before_mar15['O_ORDERKEY'].tolist()}
        }
    },
    {
        '$group': {
            '_id': {
                'L_ORDERKEY': '$L_ORDERKEY',
                'L_SHIPINSTRUCT': '$L_SHIPINSTRUCT',
                'L_SHIPMODE': '$L_SHIPMODE'
            },
            'Revenue': {
                '$sum': {
                    '$multiply': [
                        '$L_EXTENDEDPRICE',
                        {'$subtract': [1, '$L_DISCOUNT']}
                    ]
                }
            }
        }
    },
    {'$sort': SON([("Revenue", -1), ("_id.L_ORDERKEY", 1)])}
]

lineitem_agg = lineitem_col.aggregate(pipeline)

# Convert aggregation result to a DataFrame
lineitem_revenue_df = pd.DataFrame(
    [{
        'OrderKey': doc['_id']['L_ORDERKEY'], 
        'Revenue': doc['Revenue']
    } for doc in lineitem_agg]
)

# Merge the results with orders information
result = pd.merge(
    lineitem_revenue_df,
    orders_before_mar15,
    how='inner',
    left_on='OrderKey',
    right_on='O_ORDERKEY'
)

# Select and rename the necessary columns
output_df = result[['OrderKey', 'O_ORDERDATE', 'O_SHIPPRIORITY', 'Revenue']]
output_df.columns = ['OrderKey', 'OrderDate', 'ShippingPriority', 'Revenue']

# Sort according to original requirements
output_df = output_df.sort_values(['Revenue', 'OrderDate'], ascending=[False, True])

# Write the result to CSV
output_df.to_csv('query_output.csv', index=False)
