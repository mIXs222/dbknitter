# discounted_revenue_query.py

# Import necessary libraries
from pymongo import MongoClient
import pandas as pd
from direct_redis import DirectRedis

# Connect to MongoDB
client = MongoClient(host='mongodb', port=27017)
mongodb = client['tpch']
lineitem_collection = mongodb['lineitem']

# Retrieve data from MongoDB
lineitem_data = pd.DataFrame(list(lineitem_collection.find({
    'L_SHIPMODE': {'$in': ['AIR', 'AIR REG']},
    'L_SHIPINSTRUCT': 'DELIVER IN PERSON'
})))

# Connect to Redis
redis = DirectRedis(host='redis', port=6379, db=0)

# Get data from Redis
part_data = pd.read_json(redis.get('part'), typ='series').to_frame().transpose()

# Filter parts based on provided criteria and merge with lineitem data
part_filters = [
    {'P_BRAND': 'Brand#12', 'P_CONTAINER': {'$in': ['SM CASE', 'SM BOX', 'SM PACK', 'SM PKG']}, 'P_SIZE': {'$gte': 1, '$lte': 5}},
    {'P_BRAND': 'Brand#23', 'P_CONTAINER': {'$in': ['MED BAG', 'MED BOX', 'MED PKG', 'MED PACK']}, 'P_SIZE': {'$gte': 1, '$lte': 10}},
    {'P_BRAND': 'Brand#34', 'P_CONTAINER': {'$in': ['LG CASE', 'LG BOX', 'LG PACK', 'LG PKG']}, 'P_SIZE': {'$gte': 1, '$lte': 15}},
]

# Merge parts with lineitem data and calculate discounted revenue
result_data = pd.DataFrame()
for part_filter in part_filters:
    parts_subset = part_data[(part_data['P_BRAND'] == part_filter['P_BRAND']) &
                             (part_data['P_CONTAINER'].isin(part_filter['P_CONTAINER'])) &
                             (part_data['P_SIZE'] >= part_filter['P_SIZE']['$gte']) &
                             (part_data['P_SIZE'] <= part_filter['P_SIZE']['$lte'])]
    
    # Join with lineitem table and calculate
    merged_data = lineitem_data.merge(parts_subset, left_on='L_PARTKEY', right_on='P_PARTKEY')
    
    # Filtering based on L_QUANTITY for the corresponding part brand
    quantity_filter = {
        'Brand#12': (1, 11),
        'Brand#23': (10, 20),
        'Brand#34': (20, 30),
    }
    quantity_min, quantity_max = quantity_filter[part_filter['P_BRAND']]
    merged_data = merged_data[(merged_data['L_QUANTITY'] >= quantity_min) & (merged_data['L_QUANTITY'] <= quantity_max)]
    
    # Calculating discount amount and adding to result_data
    merged_data['DISCOUNTED_REVENUE'] = merged_data['L_EXTENDEDPRICE'] * (1 - merged_data['L_DISCOUNT'])
    result_data = pd.concat([result_data, merged_data])

# Write the result to CSV file
result_data.to_csv('query_output.csv', index=False)
