import pymongo
import pandas as pd
from direct_redis import DirectRedis

# Constants to establish connections
mongodb_host = 'mongodb'
mongodb_port = 27017
mongodb_db_name = 'tpch'

redis_host = 'redis'
redis_port = 6379
redis_db_name = 0

# Establish connection to MongoDB
mongo_client = pymongo.MongoClient(host=mongodb_host, port=mongodb_port)
mongo_db = mongo_client[mongodb_db_name]
mongo_part_collection = mongo_db['part']

# Query MongoDB for parts
part_query = {
    "$or": [
        {"P_BRAND": "Brand#12", "P_CONTAINER": {"$in": ["SM CASE","SM BOX","SM PACK","SM PKG"]}, "P_SIZE": {"$gte": 1, "$lte": 5}},
        {"P_BRAND": "Brand#23", "P_CONTAINER": {"$in": ["MED BAG","MED BOX","MED PKG","MED PACK"]}, "P_SIZE": {"$gte": 1, "$lte": 10}},
        {"P_BRAND": "Brand#34", "P_CONTAINER": {"$in": ["LG CASE","LG BOX","LG PACK","LG PKG"]}, "P_SIZE": {"$gte": 1, "$lte": 15}}
    ]
}
mongo_part_projection = {
    "P_PARTKEY": 1, "P_BRAND": 1, "P_CONTAINER": 1, "P_SIZE": 1
}
part_df = pd.DataFrame(list(mongo_part_collection.find(part_query, mongo_part_projection)))

# Establish connection to Redis
redis_client = DirectRedis(host=redis_host, port=redis_port, db=redis_db_name)
lineitem_df = pd.DataFrame(eval(redis_client.get('lineitem')))

# Merge the datasets and filter lineitems
result = lineitem_df.merge(part_df, left_on='L_PARTKEY', right_on='P_PARTKEY')

# Shipping by air and delivered in person
result = result[
    result['L_SHIPMODE'].isin(['AIR', 'AIR REG'])
    & ((result['P_BRAND'] == "Brand#12") & (result['L_QUANTITY'] >= 1) & (result['L_QUANTITY'] <= 11) |
       (result['P_BRAND'] == "Brand#23") & (result['L_QUANTITY'] >= 10) & (result['L_QUANTITY'] <= 20) |
       (result['P_BRAND'] == "Brand#34") & (result['L_QUANTITY'] >= 20) & (result['L_QUANTITY'] <= 30))
]

# Calculate the gross discounted revenue
result['DISCOUNTED_REVENUE'] = result['L_EXTENDEDPRICE'] * (1 - result['L_DISCOUNT'])

# Select the relevant columns per the query output requirement
output_df = result[['DISCOUNTED_REVENUE']]

# Write to CSV
output_df.to_csv('query_output.csv', index=False)
