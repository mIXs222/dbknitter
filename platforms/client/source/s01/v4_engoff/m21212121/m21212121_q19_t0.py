from pymongo import MongoClient
import pandas as pd
import direct_redis

# MongoDB connection and query
mongo_client = MongoClient('mongodb', 27017)
mongo_db = mongo_client['tpch']
lineitem_collection = mongo_db['lineitem']

# Projection might vary depending on the exact requirements, here I'm only projecting the required fields
pipeline = [
    {'$match': {
        '$or': [
            {'$and': [
                {'L_SHIPMODE': {'$in': ['AIR', 'AIR REG']}},
                {'L_SHIPINSTRUCT': 'DELIVER IN PERSON'},
            ]}
        ]
    }},
    {'$project': {
        '_id': 0,
        'L_ORDERKEY': 1,
        'L_EXTENDEDPRICE': 1,
        'L_DISCOUNT': 1
    }}
]

lineitem_data = list(lineitem_collection.aggregate(pipeline))

# Redis connection and query
redis_client = direct_redis.DirectRedis(host='redis', port=6379, db=0)

part_data = pd.read_json(redis_client.get('part'))

# Filter based on parts type conditions in pandas
type_conditions = [
    (part_data['P_BRAND'] == 'Brand#12') & (part_data['P_CONTAINER'].isin(['SM CASE', 'SM BOX', 'SM PACK', 'SM PKG'])) & (part_data['P_SIZE'] >= 1) & (part_data['P_SIZE'] <= 5),
    (part_data['P_BRAND'] == 'Brand#23') & (part_data['P_CONTAINER'].isin(['MED BAG', 'MED BOX', 'MED PKG', 'MED PACK'])) & (part_data['P_SIZE'] >= 1) & (part_data['P_SIZE'] <= 10),
    (part_data['P_BRAND'] == 'Brand#34') & (part_data['P_CONTAINER'].isin(['LG CASE', 'LG BOX', 'LG PACK', 'LG PKG'])) & (part_data['P_SIZE'] >= 1) & (part_data['P_SIZE'] <= 15),
]

filtered_parts = part_data[
    type_conditions[0] | 
    type_conditions[1] | 
    type_conditions[2]
]

# Join the data frames
filtered_parts['P_PARTKEY'] = filtered_parts['P_PARTKEY'].astype(int)
lineitem_df = pd.DataFrame(lineitem_data)
lineitem_df['L_PARTKEY'] = lineitem_df['L_PARTKEY'].astype(int)

result = lineitem_df.merge(filtered_parts, how='inner', left_on='L_PARTKEY', right_on='P_PARTKEY')

# Calculate discounted revenue
result['DISCOUNTED_REVENUE'] = result['L_EXTENDEDPRICE'] * (1 - result['L_DISCOUNT'])

# Save the result to csv file
result.to_csv('query_output.csv', index=False)
