# query_code.py
import pymongo
import pandas as pd
from direct_redis import DirectRedis

# Function to calculate the revenue
def calculate_revenue(data):
    return (data['L_EXTENDEDPRICE'] * (1 - data['L_DISCOUNT'])).sum()

# MongoDB connection
mongodb_client = pymongo.MongoClient(host='mongodb', port=27017)
mongodb_db = mongodb_client['tpch']
col_part = mongodb_db['part']

# Part filters based on brand, size, and container
part_filters = [
    {'P_BRAND': 'Brand#12', 'P_SIZE': {'$gte': 1, '$lte': 5}, 'P_CONTAINER': {'$in': ['SM CASE', 'SM BOX', 'SM PACK', 'SM PKG']}},
    {'P_BRAND': 'Brand#23', 'P_SIZE': {'$gte': 1, '$lte': 10}, 'P_CONTAINER': {'$in': ['MED BAG', 'MED BOX', 'MED PKG', 'MED PACK']}},
    {'P_BRAND': 'Brand#34', 'P_SIZE': {'$gte': 1, '$lte': 15}, 'P_CONTAINER': {'$in': ['LG CASE', 'LG BOX', 'LG PACK', 'LG PKG']}}
]

# Part keys for each filter
part_keys = [[p['P_PARTKEY'] for p in col_part.find(filter)] for filter in part_filters]

# Redis connection
redis_client = DirectRedis(host='redis', port=6379, db=0)

# Get lineitem DataFrame
lineitem_df = pd.read_msgpack(redis_client.get('lineitem'))

# Combine lineitems with part keys and calculate revenue
revenues = []
for pk in part_keys:
    # Lineitem filters based on part keys, quantity, and shipmode
    relevant_lineitem_df = lineitem_df[
        (lineitem_df['L_PARTKEY'].isin(pk)) &
        (lineitem_df['L_SHIPMODE'].isin(['AIR', 'AIR REG'])) &
        (lineitem_df['L_SHIPINSTRUCT'] == 'DELIVER IN PERSON')
    ]
    
    # Specific quantity conditions
    if pk == part_keys[0]:
        relevant_lineitem_df = relevant_lineitem_df[
            (lineitem_df['L_QUANTITY'] >= 1) & 
            (lineitem_df['L_QUANTITY'] <= 11)
        ]
    elif pk == part_keys[1]:
        relevant_lineitem_df = relevant_lineitem_df[
            (lineitem_df['L_QUANTITY'] >= 10) & 
            (lineitem_df['L_QUANTITY'] <= 20)
        ]
    elif pk == part_keys[2]:
        relevant_lineitem_df = relevant_lineitem_df[
            (lineitem_df['L_QUANTITY'] >= 20) & 
            (lineitem_df['L_QUANTITY'] <= 30)
        ]

    revenues.append(calculate_revenue(relevant_lineitem_df))

# Sum all revenues and write to file
total_revenue = sum(revenues)
output_df = pd.DataFrame({'REVENUE': [total_revenue]})
output_df.to_csv('query_output.csv', index=False)
