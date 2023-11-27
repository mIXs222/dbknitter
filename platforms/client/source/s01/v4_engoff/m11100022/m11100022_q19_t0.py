from pymongo import MongoClient
import pandas as pd
import direct_redis

# MongoDB connection
mongo_client = MongoClient('mongodb', 27017)
mongo_db = mongo_client['tpch']
part_collection = mongo_db['part']

# Redis connection
r = direct_redis.DirectRedis(host='redis', port=6379, db=0)

# Define the filters for parts according to the parameters mentioned
mongo_filters = [
    {'$or': [
        {'P_BRAND': 'Brand#12', 'P_CONTAINER': {'$in': ['SM CASE', 'SM BOX', 'SM PACK', 'SM PKG']}, 'P_SIZE': {'$gte': 1, '$lte': 5}},
        {'P_BRAND': 'Brand#23', 'P_CONTAINER': {'$in': ['MED BAG', 'MED BOX', 'MED PKG', 'MED PACK']}, 'P_SIZE': {'$gte': 1, '$lte': 10}},
        {'P_BRAND': 'Brand#34', 'P_CONTAINER': {'$in': ['LG CASE', 'LG BOX', 'LG PACK', 'LG PKG']}, 'P_SIZE': {'$gte': 1, '$lte': 15}},
    ]}
]

# Filter parts from MongoDB
part_data = part_collection.find({'$or': mongo_filters})
part_df = pd.DataFrame(list(part_data))

# Get lineitem data from Redis as a Pandas DataFrame
lineitem_data = r.get('lineitem')
lineitem_df = pd.read_json(lineitem_data)

# Join the data from both databases on P_PARTKEY = L_PARTKEY
result = pd.merge(
    part_df,
    lineitem_df,
    how='inner',
    left_on='P_PARTKEY',
    right_on='L_PARTKEY'
)

# Define lambda functions for quantity filter checks
quantity_checks = [
    lambda x: 1 <= x['L_QUANTITY'] <= 11,
    lambda x: 10 <= x['L_QUANTITY'] <= 20,
    lambda x: 20 <= x['L_QUANTITY'] <= 30,
]

# Apply quantity filters based on the corresponding P_SIZE range
result = result[(result['P_SIZE'].between(1, 5) & result.apply(quantity_checks[0], axis=1)) |
                (result['P_SIZE'].between(1, 10) & result.apply(quantity_checks[1], axis=1)) |
                (result['P_SIZE'].between(1, 15) & result.apply(quantity_checks[2], axis=1))]

# Filter for 'AIR' or 'AIR REG' SHIPMODE and calculate discounted revenue
result = result[result['L_SHIPMODE'].isin(['AIR', 'AIR REG'])]
result['DISCOUNTED_REVENUE'] = result['L_EXTENDEDPRICE'] * (1 - result['L_DISCOUNT'])

# Selecting required columns
result_final = result[['L_ORDERKEY', 'DISCOUNTED_REVENUE']]

# Write the final result to query_output.csv
result_final.to_csv('query_output.csv', index=False)
