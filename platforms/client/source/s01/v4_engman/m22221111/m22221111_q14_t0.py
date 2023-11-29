import pymongo
import pandas as pd
import redis
from datetime import datetime
from direct_redis import DirectRedis
import csv

# Connect to MongoDB
mongo_client = pymongo.MongoClient("mongodb://mongodb:27017/")
mongo_db = mongo_client["tpch"]
lineitem_collection = mongo_db["lineitem"]

# Fetch lineitem data from MongoDB
start_date = datetime(1995, 9, 1)
end_date = datetime(1995, 10, 1)

query = {
    "L_SHIPDATE": {
        "$gte": start_date,
        "$lt": end_date,
    }
}

lineitem_data = list(lineitem_collection.find(
    query,
    {
        "_id": 0,
        "L_PARTKEY": 1,
        "L_EXTENDEDPRICE": 1,
        "L_DISCOUNT": 1
    }
))

lineitem_df = pd.DataFrame(lineitem_data)

# Connect to Redis
redis_client = DirectRedis(host='redis', port=6379, db=0)

# Fetch part data from Redis
part_keys = redis_client.keys("part:*")
parts_data = []

for key in part_keys:
    part_data = redis_client.get(key)
    if part_data:
        parts_data.append(eval(part_data))

part_df = pd.DataFrame(parts_data)

# Keep only relevant part information in DataFrame
part_df = part_df[part_df["P_PARTKEY"].isin(lineitem_df["L_PARTKEY"])]

# Merge the DataFrames on P_PARTKEY and L_PARTKEY
merged_df = pd.merge(
    lineitem_df,
    part_df,
    left_on="L_PARTKEY",
    right_on="P_PARTKEY"
)

# Calculate revenue
merged_df["revenue"] = merged_df["L_EXTENDEDPRICE"] * (1 - merged_df["L_DISCOUNT"])

# Calculate total revenue
total_revenue = merged_df["revenue"].sum()

# Calculate promotion revenue
promotion_revenue = merged_df[merged_df["P_NAME"].str.contains("promo", case=False)]["revenue"].sum()

# Calculate promotion effect
promotion_effect = (promotion_revenue / total_revenue) * 100

# Output result to a file
with open('query_output.csv', 'w', newline='') as file:
    writer = csv.writer(file)
    writer.writerow(['promotion_effect'])
    writer.writerow([promotion_effect])
