import pymongo
from direct_redis import DirectRedis
import pandas as pd

# Connect to MongoDB
mongodb_client = pymongo.MongoClient("mongodb://mongodb:27017")
mongodb_db = mongodb_client["tpch"]
lineitem_collection = mongodb_db["lineitem"]

# Query MongoDB for line items
lineitem_query_conditions = [
    {"L_SHIPMODE": {"$in": ["AIR", "AIR REG"]}, "L_SHIPINSTRUCT": "DELIVER IN PERSON", "L_QUANTITY": {"$gte": 1, "$lte": 11}},
    {"L_SHIPMODE": {"$in": ["AIR", "AIR REG"]}, "L_SHIPINSTRUCT": "DELIVER IN PERSON", "L_QUANTITY": {"$gte": 10, "$lte": 20}},
    {"L_SHIPMODE": {"$in": ["AIR", "AIR REG"]}, "L_SHIPINSTRUCT": "DELIVER IN PERSON", "L_QUANTITY": {"$gte": 20, "$lte": 30}},
]

lineitem_docs = lineitem_collection.find({"$or": lineitem_query_conditions})
lineitem_df = pd.DataFrame(list(lineitem_docs))

# Connect to Redis
redis_client = DirectRedis(host="redis", port=6379, db=0)

# Get part table as a DataFrame from Redis
part_df = pd.read_json(redis_client.get("part"))

# Filter parts based on the conditions
part_query_conditions = [
    {"P_BRAND": "Brand#12", "P_CONTAINER": {"$in": ["SM CASE", "SM BOX", "SM PACK", "SM PKG"]}, "P_SIZE": {"$gte": 1, "$lte": 5}},
    {"P_BRAND": "Brand#23", "P_CONTAINER": {"$in": ["MED BAG", "MED BOX", "MED PKG", "MED PACK"]}, "P_SIZE": {"$gte": 1, "$lte": 10}},
    {"P_BRAND": "Brand#34", "P_CONTAINER": {"$in": ["LG CASE", "LG BOX", "LG PACK", "LG PKG"]}, "P_SIZE": {"$gte": 1, "$lte": 15}},
]

part_filtered_df = pd.concat([part_df[(part_df["P_BRAND"] == condition["P_BRAND"]) &
                                      (part_df["P_CONTAINER"].isin(condition["P_CONTAINER"])) &
                                      (part_df["P_SIZE"] >= condition["P_SIZE"]["$gte"]) &
                                      (part_df["P_SIZE"] <= condition["P_SIZE"]["$lte"])] for condition in part_query_conditions])

# Merge lineitem and part dataframes on partkey
combined_df = pd.merge(lineitem_df, part_filtered_df, left_on='L_PARTKEY', right_on='P_PARTKEY')

# Calculate revenue
combined_df["revenue"] = combined_df["L_EXTENDEDPRICE"] * (1 - combined_df["L_DISCOUNT"])

# Group by the required fields and sum revenue
revenue_df = combined_df.groupby(["L_PARTKEY", "P_BRAND", "P_CONTAINER"]).agg({'revenue': 'sum'}).reset_index()

# Output the results to a CSV file
revenue_df.to_csv("query_output.csv", index=False)
