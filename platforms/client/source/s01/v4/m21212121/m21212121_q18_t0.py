import pymongo
import pandas as pd
import direct_redis

# Connect to MongoDB
mongo_client = pymongo.MongoClient("mongodb://mongodb:27017/")
mongo_db = mongo_client["tpch"]
customer = pd.DataFrame(list(mongo_db["customer"].find()))
lineitem = pd.DataFrame(list(mongo_db["lineitem"].find()))

# Filter lineitem for sum of L_QUANTITY > 300
lineitem_aggregated = lineitem.groupby("L_ORDERKEY").agg({"L_QUANTITY": "sum"}).reset_index()
filtered_lineitem = lineitem_aggregated[lineitem_aggregated["L_QUANTITY"] > 300]

# Connect to Redis
redis_client = direct_redis.DirectRedis(host="redis", port=6379, db=0)
# Read the orders table from Redis into a Pandas DataFrame
orders_df_str = redis_client.get("orders")
orders = pd.read_json(orders_df_str)

# Merge the dataframes to simulate a join operation
merged_df = customer.merge(orders, left_on="C_CUSTKEY", right_on="O_CUSTKEY")
merged_df = merged_df.merge(filtered_lineitem, left_on="O_ORDERKEY", right_on="L_ORDERKEY")

# Group by the required fields
final_df = merged_df.groupby(["C_NAME", "C_CUSTKEY", "O_ORDERKEY", "O_ORDERDATE", "O_TOTALPRICE"]) \
    .agg({"L_QUANTITY": "sum"}) \
    .reset_index()

# Sort by O_TOTALPRICE descending and O_ORDERDATE
final_df.sort_values(by=["O_TOTALPRICE", "O_ORDERDATE"], ascending=[False, True], inplace=True)

# Write to CSV file
final_df.to_csv('query_output.csv', index=False)
