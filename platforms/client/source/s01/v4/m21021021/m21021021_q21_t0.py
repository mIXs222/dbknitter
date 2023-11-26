import pymongo
import pandas as pd
from direct_redis import DirectRedis

# Connect to the MongoDB database
mongo_client = pymongo.MongoClient("mongodb://mongodb:27017/")
mongo_db = mongo_client["tpch"]
lineitem_collection = mongo_db["lineitem"]

# Convert the MongoDB lineitem collection to a DataFrame
lineitem_df = pd.DataFrame(list(lineitem_collection.find()))

# Connect to the Redis database
redis_client = DirectRedis(host='redis', port=6379)

# Convert Redis datasets to DataFrames
nation_df = pd.read_json(redis_client.get('nation'))
supplier_df = pd.read_json(redis_client.get('supplier'))
orders_df = pd.read_json(redis_client.get('orders'))

# Apply the filters and transformations
result_df = (
    supplier_df[supplier_df["S_NATIONKEY"] == nation_df[nation_df["N_NAME"] == "SAUDI ARABIA"]["N_NATIONKEY"].iloc[0]]
    .merge(lineitem_df, left_on="S_SUPPKEY", right_on="L_SUPPKEY")
    .merge(orders_df[orders_df["O_ORDERSTATUS"] == "F"], left_on="L_ORDERKEY", right_on="O_ORDERKEY")
)

# Apply EXISTS conditions by filtering L1
l1_df = result_df[(result_df["L_RECEIPTDATE"] > result_df["L_COMMITDATE"])]

# Apply EXISTS subquery L2 condition
l2_cond = (
    lineitem_df.groupby("L_ORDERKEY")["L_SUPPKEY"]
    .apply(lambda x: (x != l1_df["L_SUPPKEY"]).any())
    .reset_index(name="L2_exists")
)
l1_df = l1_df.merge(l2_cond, on="L_ORDERKEY")

# Apply NOT EXISTS subquery L3 condition
l3_cond = (
    lineitem_df[lineitem_df["L_RECEIPTDATE"] > lineitem_df["L_COMMITDATE"]]
    .groupby("L_ORDERKEY")["L_SUPPKEY"]
    .apply(lambda x: (x != l1_df["L_SUPPKEY"]).any())
    .reset_index(name="L3_not_exists")
)

l3_cond["L3_not_exists"] = ~l3_cond["L3_not_exists"]
l1_df = l1_df.merge(l3_cond, on="L_ORDERKEY")

# Aggregate to get the count and sort
output_df = (
    l1_df[l1_df["L2_exists"] & l1_df["L3_not_exists"]]
    .groupby("S_NAME")["L_ORDERKEY"]
    .count()
    .reset_index(name="NUMWAIT")
    .sort_values(by=["NUMWAIT", "S_NAME"], ascending=[False, True])
)

# Write to CSV
output_df.to_csv("query_output.csv", index=False)
