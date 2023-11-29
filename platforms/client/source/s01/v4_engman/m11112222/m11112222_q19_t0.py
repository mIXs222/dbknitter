import pymongo
import pandas as pd

# Database Connection Information
mongodb_connection_info = {
    'hostname': 'mongodb',
    'port': 27017,
    'db_name': 'tpch'
}

# MongoDB Connection
client = pymongo.MongoClient(
    host=mongodb_connection_info['hostname'],
    port=mongodb_connection_info['port']
)

db = client[mongodb_connection_info['db_name']]

# Fetching MongoDB data for 'part'
part_collection = db['part']
parts_data_from_mongodb = pd.DataFrame(list(part_collection.find()))

# Defining part types for selection
part_types = {
    1: {"brand_id": "12", "containers": ['SM CASE', 'SM BOX', 'SM PACK', 'SM PKG'], "size": range(1, 6)},
    2: {"brand_id": "23", "containers": ['MED BAG', 'MED BOX', 'MED PKG', 'MED PACK'], "size": range(1, 11)},
    3: {"brand_id": "34", "containers": ['LG CASE', 'LG BOX', 'LG PACK', 'LG PKG'], "size": range(1, 16)}
}

# Filtering parts from MongoDB based on part types
part_keys = []
for p_type, p_info in part_types.items():
    filtered_parts = parts_data_from_mongodb[
        (parts_data_from_mongodb['P_BRAND'].apply(lambda x: x.endswith(p_info['brand_id'])) &
        (parts_data_from_mongodb['P_CONTAINER'].isin(p_info['containers'])) &
        (parts_data_from_mongodb['P_SIZE'].between(min(p_info['size']), max(p_info['size']), inclusive="both")))
    ]
    part_keys.extend(filtered_parts['P_PARTKEY'].values.tolist())

# Connect to Redis
from direct_redis import DirectRedis
redis_connection_info = {
    'hostname': 'redis',
    'port': 6379,
    'db_name': 0
}

r = DirectRedis(
    host=redis_connection_info['hostname'], 
    port=redis_connection_info['port'], 
    db=redis_connection_info['db_name']
)

# Fetch Redis data for 'lineitem'
lineitem_data_from_redis = r.get('lineitem')

# If the data is a string representing a dataframe, we turn it back into a dataframe
if isinstance(lineitem_data_from_redis, bytes):
    lineitem_data_from_redis = pd.read_msgpack(lineitem_data_from_redis)

# Filtering lineitems based on quantity, shipmode, and part keys
filtered_lineitems = lineitem_data_from_redis[
    (lineitem_data_from_redis['L_SHIPMODE'].isin(['AIR', 'AIR REG'])) &
    (lineitem_data_from_redis['L_PARTKEY'].isin(part_keys))
]

# Calculate discounted revenue and sum
filtered_lineitems['REVENUE'] = filtered_lineitems['L_EXTENDEDPRICE'] * (1 - filtered_lineitems['L_DISCOUNT'])
total_revenue = filtered_lineitems['REVENUE'].sum()

# Save the result to a CSV file
result_df = pd.DataFrame({'REVENUE': [total_revenue]})
result_df.to_csv('query_output.csv', index=False)
