# discount_revenue_query.py
import pymongo
import pandas as pd
from direct_redis import DirectRedis

# MongoDB connection
mongo_client = pymongo.MongoClient("mongodb", 27017)
mongodb = mongo_client["tpch"]
part_collection = mongodb["part"]

# Redis connection
redis_client = DirectRedis(host="redis", port=6379, db=0)

# Perform MongoDB query
part_query = {
    'P_BRAND': {'$in': ['Brand#12', 'Brand#23', 'Brand#34']},
    'P_CONTAINER': {'$in': ['SM CASE', 'SM BOX', 'SM PACK', 'SM PKG', 'MED BAG', 'MED BOX', 'MED PKG', 'MED PACK', 'LG CASE', 'LG BOX', 'LG PACK', 'LG PKG']},
    'P_SIZE': {'$gte': 1}
}
parts = part_collection.find(part_query, projection={'_id': False, 'P_PARTKEY': True, 'P_BRAND': True, 'P_CONTAINER': True, 'P_SIZE': True})
parts_df = pd.DataFrame(list(parts))

# Filter retrieved parts according to the brand, container, and size criteria
parts_df = parts_df[
    ((parts_df['P_BRAND'] == 'Brand#12') & (parts_df['P_CONTAINER'].isin(['SM CASE', 'SM BOX', 'SM PACK', 'SM PKG'])) & (parts_df['P_SIZE'] >= 1) & (parts_df['P_SIZE'] <= 5)) |
    ((parts_df['P_BRAND'] == 'Brand#23') & (parts_df['P_CONTAINER'].isin(['MED BAG', 'MED BOX', 'MED PKG', 'MED PACK'])) & (parts_df['P_SIZE'] >= 1) & (parts_df['P_SIZE'] <= 10)) |
    ((parts_df['P_BRAND'] == 'Brand#34') & (parts_df['P_CONTAINER'].isin(['LG CASE', 'LG BOX', 'LG PACK', 'LG PKG'])) & (parts_df['P_SIZE'] >= 1) & (parts_df['P_SIZE'] <= 15))
]

# Perform Redis query
lineitem_df = pd.read_json(redis_client.get('lineitem'), orient='records')
lineitem_df = lineitem_df[lineitem_df['L_SHIPMODE'].isin(['AIR', 'AIR REG'])]
lineitem_df['delivery_flag'] = lineitem_df['L_SHIPINSTRUCT'].apply(lambda x: 'DELIVER IN PERSON' in x)

# Calculate the discounted revenue
lineitem_df['revenue'] = lineitem_df['L_EXTENDEDPRICE'] * (1 - lineitem_df['L_DISCOUNT'])

# Merge Part and Lineitem dataframes based on part key
merged_df = lineitem_df.merge(parts_df, how='inner', left_on='L_PARTKEY', right_on='P_PARTKEY')

# Apply additional filters on quantity and select only relevant columns
filtered_df = merged_df[
    ((merged_df['P_BRAND'] == 'Brand#12') & (merged_df['L_QUANTITY'] >= 1) & (merged_df['L_QUANTITY'] <= 11)) |
    ((merged_df['P_BRAND'] == 'Brand#23') & (merged_df['L_QUANTITY'] >= 10) & (merged_df['L_QUANTITY'] <= 20)) |
    ((merged_df['P_BRAND'] == 'Brand#34') & (merged_df['L_QUANTITY'] >= 20) & (merged_df['L_QUANTITY'] <= 30))
]

# Filter for 'delivery in person'
final_df = filtered_df[filtered_df['delivery_flag'] == True]

# Write the final output to a CSV file
final_output = final_df[['L_ORDERKEY', 'revenue']]
final_output.to_csv('query_output.csv', index=False)
