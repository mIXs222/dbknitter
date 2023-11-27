import pymongo
from pymongo import MongoClient
import pandas as pd
import direct_redis

# Connect to MongoDB
client = MongoClient('mongodb', 27017)
db = client['tpch']

# Connect to Redis
r = direct_redis.DirectRedis(host='redis', port=6379, db=0)

# MongoDB Query
part_projection = {
    "_id": False,
    "P_PARTKEY": True,
    "P_BRAND": True,
    "P_TYPE": True,
    "P_SIZE": True,
    "P_CONTAINER": True
}
mongo_query = {
    '$or': [
        {'$and': [
            {'P_BRAND': 'Brand#12'}, 
            {'P_CONTAINER': {'$in': ['SM CASE', 'SM BOX', 'SM PACK', 'SM PKG']}},
            {'P_SIZE': {'$gte': 1, '$lte': 5}}
        ]},
        {'$and': [
            {'P_BRAND': 'Brand#23'}, 
            {'P_CONTAINER': {'$in': ['MED BAG', 'MED BOX', 'MED PKG', 'MED PACK']}},
            {'P_SIZE': {'$gte': 1, '$lte': 10}}
        ]},
        {'$and': [
            {'P_BRAND': 'Brand#34'}, 
            {'P_CONTAINER': {'$in': ['LG CASE', 'LG BOX', 'LG PACK', 'LG PKG']}},
            {'P_SIZE': {'$gte': 1, '$lte': 15}}
        ]}
    ]
}
part_df = pd.DataFrame(list(db['part'].find(mongo_query, part_projection)))

# Redis Query
query_output = []
lineitem_df = pd.read_msgpack(r.get('lineitem'))
for index, row in lineitem_df.iterrows():
    if not ((row['L_SHIPMODE'] == 'AIR' or row['L_SHIPMODE'] == 'AIR REG') and row['L_SHIPINSTRUCT'] == 'DELIVER IN PERSON'):
        continue
    part_matched = part_df[part_df['P_PARTKEY'] == row['L_PARTKEY']]
    if part_matched.empty:
        continue
    if part_matched.iloc[0]['P_CONTAINER'] not in row['L_CONTAINER']:
        continue
    if not ((part_matched.iloc[0]['P_SIZE'] >= 1 and part_matched.iloc[0]['P_SIZE'] <= 5 and row['L_QUANTITY'] >= 1 and row['L_QUANTITY'] <= 11) or
            (part_matched.iloc[0]['P_SIZE'] >= 1 and part_matched.iloc[0]['P_SIZE'] <= 10 and row['L_QUANTITY'] >= 10 and row['L_QUANTITY'] <= 20) or
            (part_matched.iloc[0]['P_SIZE'] >= 1 and part_matched.iloc[0]['P_SIZE'] <= 15 and row['L_QUANTITY'] >= 20 and row['L_QUANTITY'] <= 30)):
        continue
    discounted_price = row['L_EXTENDEDPRICE'] * (1 - row['L_DISCOUNT'])
    query_output.append((row['L_ORDERKEY'], row['L_PARTKEY'], discounted_price))

# Saving result to query_output.csv
output_df = pd.DataFrame(query_output, columns=['L_ORDERKEY', 'L_PARTKEY', 'DISCOUNTED_REVENUE'])
output_df.to_csv('query_output.csv', index=False)
