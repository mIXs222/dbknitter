# import needed libraries
import pymongo
import direct_redis
import pandas as pd

# Connect to mongodb
mongo_client = pymongo.MongoClient("mongodb://mongodb:27017/")
mongo_db = mongo_client["tpch"]
part_collection = mongo_db["part"]

# Query mongodb for BRAND#23 with MED BAG parts
part_query = {"P_BRAND": "BRAND#23", "P_CONTAINER": "MED BAG"}
part_projection = {"P_PARTKEY": 1, "_id": 0}
part_keys = set(doc['P_PARTKEY'] for doc in part_collection.find(part_query, part_projection))

# Connect to redis
redis_client = direct_redis.DirectRedis(host="redis", port=6379, db=0)
lineitem_df = redis_client.get('lineitem')

# Filter lineitem DataFrame to include only relevant part keys
lineitem_df = lineitem_df[lineitem_df["L_PARTKEY"].isin(part_keys)]

# Determine the average quantity and filter lineitems with quantity < 20% of average
average_quantity = lineitem_df["L_QUANTITY"].mean()
filtered_lineitem_df = lineitem_df[lineitem_df["L_QUANTITY"] < 0.2 * average_quantity]

# Calculate yearly loss in revenue - assuming 7 years database
filtered_lineitem_df["loss"] = filtered_lineitem_df["L_EXTENDEDPRICE"] * (1 - filtered_lineitem_df["L_DISCOUNT"])
total_loss = filtered_lineitem_df["loss"].sum()
average_yearly_loss = total_loss / 7

# Write result to CSV
output_df = pd.DataFrame([{'average_yearly_loss': average_yearly_loss}])
output_df.to_csv('query_output.csv', index=False)
