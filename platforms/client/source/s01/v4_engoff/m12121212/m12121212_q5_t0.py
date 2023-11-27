# import necessary libraries
import pymongo
from datetime import datetime
import direct_redis
import pandas as pd

# Connection to MongoDB
mongo_client = pymongo.MongoClient("mongodb://mongodb:27017/")
mongo_db = mongo_client["tpch"]
mongo_nation = mongo_db["nation"]
mongo_orders = mongo_db["orders"]

# Get nations from Asia
asia_nations = list(mongo_nation.find({"N_REGIONKEY": 3}, {"_id": 0, "N_NATIONKEY": 1, "N_NAME": 1}))

# Convert nation data to pandas DataFrame
df_nations = pd.DataFrame(asia_nations)

# Get orders between the dates 1990-01-01 and 1995-01-01
date_low = datetime(1990, 1, 1)
date_high = datetime(1995, 1, 1)
orders_cursor = mongo_orders.find({"O_ORDERDATE": {"$gte": date_low, "$lt": date_high}}, {"_id": 0})
df_orders = pd.DataFrame(list(orders_cursor))

# Connection to Redis
direct_redis_conn = direct_redis.DirectRedis(host="redis", port=6379, db=0)

# Get lineitem and customer data from Redis
df_lineitem = pd.read_json(direct_redis_conn.get('lineitem'))
df_customer = pd.read_json(direct_redis_conn.get('customer'))

# filter customers belonging to nations in ASIA
df_customer_asia = df_customer[df_customer.C_NATIONKEY.isin(df_nations.N_NATIONKEY)]

# Merging tables
df_merged = pd.merge(df_customer_asia, df_orders, left_on="C_CUSTKEY", right_on="O_CUSTKEY")
df_merged = pd.merge(df_merged, df_lineitem, left_on="O_ORDERKEY", right_on="L_ORDERKEY")

# Computing revenue
df_merged["revenue"] = df_merged["L_EXTENDEDPRICE"] * (1 - df_merged["L_DISCOUNT"])
df_revenue = df_merged.groupby("C_NATIONKEY")["revenue"].sum().reset_index()

# Merging with nation names
df_result = pd.merge(df_revenue, df_nations, left_on="C_NATIONKEY", right_on="N_NATIONKEY")

# Selecting required columns and sorting by revenue in descending order
df_result = df_result[["N_NAME", "revenue"]].sort_values(by="revenue", ascending=False)

# Write output to CSV
df_result.to_csv("query_output.csv", index=False)
