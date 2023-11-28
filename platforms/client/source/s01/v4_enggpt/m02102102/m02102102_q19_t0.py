import pymongo
import pandas as pd
from bson import json_util
import direct_redis

# MongoDB connection and fetching part table
mongo_client = pymongo.MongoClient("mongodb", 27017)
mongo_db = mongo_client["tpch"]
part_collection = mongo_db["part"]

part_query = {
    "$or": [
        {
            "P_BRAND": "Brand#12",
            "P_CONTAINER": {"$in": ["SM CASE", "SM BOX", "SM PACK", "SM PKG"]},
            "P_SIZE": {"$gte": 1, "$lte": 5},
        },
        {
            "P_BRAND": "Brand#23",
            "P_CONTAINER": {"$in": ["MED BAG", "MED BOX", "MED PKG", "MED PACK"]},
            "P_SIZE": {"$gte": 1, "$lte": 10},
        },
        {
            "P_BRAND": "Brand#34",
            "P_CONTAINER": {"$in": ["LG CASE", "LG BOX", "LG PACK", "LG PKG"]},
            "P_SIZE": {"$gte": 1, "$lte": 15},
        },
    ]
}
part_projection = {
    "_id": False,
    "P_PARTKEY": True,
    "P_BRAND": True,
    "P_CONTAINER": True,
    "P_SIZE": True,
}

part_cursor = part_collection.find(part_query, part_projection)
part_df = pd.DataFrame(list(part_cursor))

# Redis connection and fetching lineitem table
redis_client = direct_redis.DirectRedis("redis", 6379)

lineitem_json = redis_client.get("lineitem")
lineitem_df = pd.read_json(lineitem_json, orient='records')

# Filtering lineitem data based on the conditions
lineitem_filtered_df = lineitem_df[
    (
        ((lineitem_df['L_QUANTITY'] >= 1) & (lineitem_df['L_QUANTITY'] <= 11)) |
        ((lineitem_df['L_QUANTITY'] >= 10) & (lineitem_df['L_QUANTITY'] <= 20)) |
        ((lineitem_df['L_QUANTITY'] >= 20) & (lineitem_df['L_QUANTITY'] <= 30))
    ) &
    lineitem_df['L_SHIPMODE'].isin(['AIR', 'AIR REG']) &
    (lineitem_df['L_SHIPINSTRUCT'] == 'DELIVER IN PERSON')
]

# Combine dataframes based on the part key
combined_df = lineitem_filtered_df.merge(part_df, left_on='L_PARTKEY', right_on='P_PARTKEY')

# Calculate revenue
combined_df['REVENUE'] = combined_df['L_EXTENDEDPRICE'] * (1 - combined_df['L_DISCOUNT'])

# Sum revenue for each brand and container
revenue_df = combined_df.groupby(['P_BRAND', 'P_CONTAINER']).agg(TOTAL_REVENUE=pd.NamedAgg(column='REVENUE', aggfunc='sum')).reset_index()

# Write to csv
revenue_df.to_csv("query_output.csv", index=False)
