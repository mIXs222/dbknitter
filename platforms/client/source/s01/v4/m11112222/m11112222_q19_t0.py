import pymongo
import pandas as pd
from direct_redis import DirectRedis

# Connect to MongoDB
mongo_client = pymongo.MongoClient("mongodb://mongodb:27017/")
mongodb = mongo_client["tpch"]
part_collection = mongodb["part"]

# Extract the 'part' table data from MongoDB
part_data = pd.DataFrame(list(part_collection.find({
    "$or": [
        {"P_BRAND": "Brand#12", "P_SIZE": {"$gte": 1, "$lte": 5}, "P_CONTAINER": {"$in": ["SM CASE", "SM BOX", "SM PACK", "SM PKG"]}},
        {"P_BRAND": "Brand#23", "P_SIZE": {"$gte": 1, "$lte": 10}, "P_CONTAINER": {"$in": ["MED BAG", "MED BOX", "MED PKG", "MED PACK"]}},
        {"P_BRAND": "Brand#34", "P_SIZE": {"$gte": 1, "$lte": 15}, "P_CONTAINER": {"$in": ["LG CASE", "LG BOX", "LG PACK", "LG PKG"]}}
    ]
})))

# Connect to Redis
redis_client = DirectRedis(host="redis", port=6379, db=0)

# Extract 'lineitem' table data from Redis into a DataFrame
lineitem_data = pd.read_json(redis_client.get('lineitem'))

# Merging the data and filtering according to the SQL WHERE clause 
combined_data = pd.merge(
    lineitem_data,
    part_data,
    how='inner',
    left_on='L_PARTKEY',
    right_on='P_PARTKEY'
)

# Filtering the merged data
result_data = combined_data[
    ((combined_data['P_BRAND'] == 'Brand#12') &
     (combined_data['P_CONTAINER'].isin(['SM CASE', 'SM BOX', 'SM PACK', 'SM PKG'])) &
     (combined_data['L_QUANTITY'] >= 1) & (combined_data['L_QUANTITY'] <= 11) &
     (combined_data['P_SIZE'] >= 1) & (combined_data['P_SIZE'] <= 5) &
     (combined_data['L_SHIPMODE'].isin(['AIR', 'AIR REG'])) &
     (combined_data['L_SHIPINSTRUCT'] == 'DELIVER IN PERSON')) |

    ((combined_data['P_BRAND'] == 'Brand#23') &
     (combined_data['P_CONTAINER'].isin(['MED BAG', 'MED BOX', 'MED PKG', 'MED PACK'])) &
     (combined_data['L_QUANTITY'] >= 10) & (combined_data['L_QUANTITY'] <= 20) &
     (combined_data['P_SIZE'] >= 1) & (combined_data['P_SIZE'] <= 10) &
     (combined_data['L_SHIPMODE'].isin(['AIR', 'AIR REG'])) &
     (combined_data['L_SHIPINSTRUCT'] == 'DELIVER IN PERSON')) |

    ((combined_data['P_BRAND'] == 'Brand#34') &
     (combined_data['P_CONTAINER'].isin(['LG CASE', 'LG BOX', 'LG PACK', 'LG PKG'])) &
     (combined_data['L_QUANTITY'] >= 20) & (combined_data['L_QUANTITY'] <= 30) &
     (combined_data['P_SIZE'] >= 1) & (combined_data['P_SIZE'] <= 15) &
     (combined_data['L_SHIPMODE'].isin(['AIR', 'AIR REG'])) &
     (combined_data['L_SHIPINSTRUCT'] == 'DELIVER IN PERSON'))
]

# Calculate REVENUE
result_data['REVENUE'] = result_data['L_EXTENDEDPRICE'] * (1 - result_data['L_DISCOUNT'])

# Group by to sum REVENUE
result = result_data.groupby(by=lambda x: True).agg({'REVENUE': 'sum'}).reset_index(drop=True)

# Write to CSV
result.to_csv('query_output.csv', index=False)
