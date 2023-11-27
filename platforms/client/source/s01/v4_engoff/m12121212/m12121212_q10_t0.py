import pymongo
import pandas as pd
from datetime import datetime
from direct_redis import DirectRedis

# MongoDB connection
client = pymongo.MongoClient("mongodb", 27017)
mongodb = client["tpch"]
collection_nation = mongodb["nation"]
collection_orders = mongodb["orders"]

# Redis connection
redis = DirectRedis(host='redis', port=6379, db=0)

# Fetch data from MongoDB
query_orders = {
    "O_ORDERDATE": {
        "$gte": datetime(1993, 10, 1),
        "$lt": datetime(1994, 1, 1)
    }
}
orders_df = pd.DataFrame(list(collection_orders.find(query_orders, projection={"_id": False})))

# Fetch data from Redis
customer_df = pd.read_json(redis.get('customer'), orient='records')
lineitem_df = pd.read_json(redis.get('lineitem'), orient='records')

# Filter relevant orders
orders_df = orders_df[orders_df["O_ORDERSTATUS"] == "R"]

# Compute lost revenue
lineitem_df["LOST_REVENUE"] = lineitem_df["L_EXTENDEDPRICE"] * (1 - lineitem_df["L_DISCOUNT"])

# Join customer with nation
nation_df = pd.DataFrame(list(collection_nation.find({}, projection={"_id": False})))
customer_with_nation = pd.merge(customer_df, nation_df, left_on="C_NATIONKEY", right_on="N_NATIONKEY")

# Join orders with lineitems and compute total lost revenue per customer
orders_with_lineitems = pd.merge(orders_df, lineitem_df, left_on="O_ORDERKEY", right_on="L_ORDERKEY")
total_lost_revenue = orders_with_lineitems.groupby("O_CUSTKEY")["LOST_REVENUE"].sum().reset_index()

# Join total lost revenue with customer information
result_df = pd.merge(total_lost_revenue, customer_with_nation, left_on="O_CUSTKEY", right_on="C_CUSTKEY")

# Select relevant columns and sort as per requirement
result_df = result_df[['C_NAME', 'C_ADDRESS', 'N_NAME', 'C_PHONE', 'C_ACCTBAL', 'C_COMMENT', 'LOST_REVENUE']]
result_df.sort_values(by=['LOST_REVENUE', 'C_CUSTKEY', 'C_NAME', 'C_ACCTBAL'], ascending=[False, True, True, True], inplace=True)

# Write to CSV
result_df.to_csv("query_output.csv", index=False)
