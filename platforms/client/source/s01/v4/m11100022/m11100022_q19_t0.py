import pymongo
import pandas as pd
from direct_redis import DirectRedis

# MongoDB connection
client = pymongo.MongoClient('mongodb', 27017)
mongo_db = client['tpch']

# MongoDB query for parts
part_conditions = {
    '$or': [
        {'$and': [
            {'P_BRAND': 'Brand#12'},
            {'P_CONTAINER': {'$in': ['SM CASE', 'SM BOX', 'SM PACK', 'SM PKG']}},
            {'P_SIZE': {'$gte': 1, '$lte': 5}},
        ]},
        {'$and': [
            {'P_BRAND': 'Brand#23'},
            {'P_CONTAINER': {'$in': ['MED BAG', 'MED BOX', 'MED PKG', 'MED PACK']}},
            {'P_SIZE': {'$gte': 1, '$lte': 10}},
        ]},
        {'$and': [
            {'P_BRAND': 'Brand#34'},
            {'P_CONTAINER': {'$in': ['LG CASE', 'LG BOX', 'LG PACK', 'LG PKG']}},
            {'P_SIZE': {'$gte': 1, '$lte': 15}},
        ]}
    ]
}

part_projection = {
    'P_PARTKEY': 1,
    '_id': 0
}
part_df = pd.DataFrame(list(mongo_db.part.find(part_conditions, part_projection)))

# Redis connection
redis = DirectRedis(host='redis', port=6379, db=0)
lineitem_df = pd.read_msgpack(redis.get('lineitem'))
lineitem_df['L_EXTENDEDPRICE'] = lineitem_df['L_EXTENDEDPRICE'].astype(float)
lineitem_df['L_DISCOUNT'] = lineitem_df['L_DISCOUNT'].astype(float)

# Merge DataFrames
merged_df = part_df.merge(lineitem_df, left_on='P_PARTKEY', right_on='L_PARTKEY')

# Filter lineitem conditions and calculate
merged_df = merged_df[
    ((merged_df['P_BRAND'] == 'Brand#12') &
    (merged_df['L_QUANTITY'] >= 1) & (merged_df['L_QUANTITY'] <= 11) &
    (merged_df['L_SHIPMODE'].isin(['AIR', 'AIR REG'])) &
    (merged_df['L_SHIPINSTRUCT'] == 'DELIVER IN PERSON')) |

    ((merged_df['P_BRAND'] == 'Brand#23') &
    (merged_df['L_QUANTITY'] >= 10) & (merged_df['L_QUANTITY'] <= 20) &
    (merged_df['L_SHIPMODE'].isin(['AIR', 'AIR REG'])) &
    (merged_df['L_SHIPINSTRUCT'] == 'DELIVER IN PERSON')) |

    ((merged_df['P_BRAND'] == 'Brand#34') &
    (merged_df['L_QUANTITY'] >= 20) & (merged_df['L_QUANTITY'] <= 30) &
    (merged_df['L_SHIPMODE'].isin(['AIR', 'AIR REG'])) &
    (merged_df['L_SHIPINSTRUCT'] == 'DELIVER IN PERSON'))
]

# Calculate revenue
merged_df['REVENUE'] = merged_df['L_EXTENDEDPRICE'] * (1 - merged_df['L_DISCOUNT'])
revenue = merged_df['REVENUE'].sum()

# Write result to CSV
pd.DataFrame({'REVENUE': [revenue]}).to_csv('query_output.csv', index=False)
