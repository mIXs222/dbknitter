# File: query.py

import pymongo
import pandas as pd
import csv
from direct_redis import DirectRedis

# MongoDB connection
client = pymongo.MongoClient("mongodb", 27017)
mongodb = client["tpch"]
customer = pd.DataFrame(list(mongodb.customer.find({"C_MKTSEGMENT": "BUILDING"})))

# Redis connection
redis_client = DirectRedis(host="redis", port=6379, db=0)
orders_df_json = redis_client.get("orders")
lineitem_df_json = redis_client.get("lineitem")
orders = pd.read_json(orders_df_json)
lineitem = pd.read_json(lineitem_df_json)

# Filter lineitem for date conditions
lineitem_filtered = lineitem[
    (lineitem["L_SHIPDATE"] > "1995-03-15") & (lineitem["L_ORDERKEY"].isin(orders["O_ORDERKEY"]))
]

# Calculate revenue
lineitem_filtered["REVENUE"] = lineitem_filtered["L_EXTENDEDPRICE"] * (1 - lineitem_filtered["L_DISCOUNT"])

# Aggregate revenue per order
revenue_per_order = lineitem_filtered.groupby("L_ORDERKEY", as_index=False)["REVENUE"].sum()

# Merge orders with customer data
orders_filtered = orders[
    (orders["O_ORDERDATE"] < "1995-03-05") &
    (orders["O_CUSTKEY"].isin(customer["C_CUSTKEY"]))
].merge(revenue_per_order, left_on="O_ORDERKEY", right_on="L_ORDERKEY")

# Select required columns and sort
result = orders_filtered[["O_ORDERKEY", "REVENUE", "O_ORDERDATE", "O_SHIPPRIORITY"]].sort_values(by=["REVENUE"], ascending=False)

# Write to CSV
result.to_csv("query_output.csv", index=False, quoting=csv.QUOTE_NONNUMERIC)
