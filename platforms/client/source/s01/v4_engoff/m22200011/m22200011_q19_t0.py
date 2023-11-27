import pandas as pd
from pymongo import MongoClient
from direct_redis import DirectRedis

# Constants:
mongodb_host = "mongodb"
mongodb_port = 27017
redis_host = "redis"
redis_port = 6379
mongodb_db_name = "tpch"
redis_db_name = 0

# MongoDB Connection:
mongo_client = MongoClient(host=mongodb_host, port=mongodb_port)
mongodb = mongo_client[mongodb_db_name]

# Redis Connection:
redis_client = DirectRedis(host=redis_host, port=redis_port, db=redis_db_name)

# Load MongoDB data:
lineitem = pd.DataFrame(list(mongodb.lineitem.find()))

# Load Redis data:
part_df = pd.DataFrame(eval(redis_client.get('part')))

# Part selection based on criteria:
brand_containers = {
    12: ['SM CASE', 'SM BOX', 'SM PACK', 'SM PKG'],
    23: ['MED BAG', 'MED BOX', 'MED PKG', 'MED PACK'],
    34: ['LG CASE', 'LG BOX', 'LG PACK', 'LG PKG']
}

size_ranges = {
    12: (1, 5),
    23: (1, 10),
    34: (1, 15)
}

quantity_ranges = {
    12: (1, 11),
    23: (10, 20),
    34: (20, 30)
}

# Filter parts
filtered_parts = part_df[part_df.apply(
    lambda row: row['P_CONTAINER'] in brand_containers.get(row['P_BRAND'], []) and
                size_ranges[row['P_BRAND']][0] <= row['P_SIZE'] <= size_ranges[row['P_BRAND']][1],
    axis=1
)]

# Map Part Keys to Brands for Filtering Lineitems
part_key_to_brand = filtered_parts.set_index('P_PARTKEY')['P_BRAND'].to_dict()

# Filter Lineitems
filtered_lineitems = lineitem[lineitem.apply(
    lambda row: row['L_PARTKEY'] in part_key_to_brand and
                quantity_ranges[part_key_to_brand[row['L_PARTKEY']]][0] <= row['L_QUANTITY'] <= quantity_ranges[part_key_to_brand[row['L_PARTKEY']]][1] and
                row['L_SHIPMODE'] in ['AIR', 'AIR REG'],
    axis=1
)]

# Calculate Gross Discounted Revenue
filtered_lineitems['REVENUE'] = filtered_lineitems.apply(
    lambda row: (row['L_EXTENDEDPRICE'] * (1 - row['L_DISCOUNT'])),
    axis=1
)

# Save query's results to CSV
filtered_lineitems.to_csv('query_output.csv', index=False)
