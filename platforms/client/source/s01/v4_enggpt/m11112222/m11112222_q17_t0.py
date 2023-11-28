from pymongo import MongoClient
import pandas as pd
import direct_redis
import csv

# Connection to MongoDB
mongo_client = MongoClient('mongodb', 27017)
mongodb = mongo_client['tpch']
part_collection = mongodb['part']

# Query parts from MongoDB
pipeline = [
    {'$project': {
        '_id': 0,
        'P_PARTKEY': 1,
        'P_NAME': 1,
        'P_MFGR': 1,
        'P_BRAND': 1,
        'P_TYPE': 1,
        'P_SIZE': 1,
        'P_CONTAINER': 1,
        'P_RETAILPRICE': 1,
        'P_COMMENT': 1
    }},
    {'$match': {
        'P_BRAND': 'Brand#23',
        'P_CONTAINER': 'MED BAG'
    }}
]

parts_df = pd.DataFrame(list(part_collection.aggregate(pipeline)))

# Connection to Redis
r = direct_redis.DirectRedis(host='redis', port=6379, db=0)

# Query lineitem from Redis
lineitem_df = pd.read_json(r.get('lineitem'))

# Merge and perform the analysis
result = pd.merge(
    lineitem_df,
    parts_df,
    how='inner',
    left_on='L_PARTKEY',
    right_on='P_PARTKEY'
)

# Calculate the average quantity for each part
avg_quantity = result.groupby('L_PARTKEY')['L_QUANTITY'].mean().reset_index()
avg_quantity.rename(columns={'L_QUANTITY': 'AVG_QUANTITY'}, inplace=True)

# Join to filter lineitem by 20% of the average quantity
result = pd.merge(
    result,
    avg_quantity,
    how='left',
    on='L_PARTKEY'
)
result = result[result['L_QUANTITY'] < (result['AVG_QUANTITY'] * 0.20)]

# Calculate the average yearly extended price
result['YEARLY_EXTENDEDPRICE'] = result['L_EXTENDEDPRICE'] / 7.0
final_result = result.groupby(['P_BRAND', 'P_CONTAINER'])['YEARLY_EXTENDEDPRICE'].mean().reset_index()

# Write to CSV
final_result.to_csv('query_output.csv', index=False)
