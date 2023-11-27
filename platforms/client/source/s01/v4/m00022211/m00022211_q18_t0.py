# query_script.py
import pymongo
import pandas as pd
from bson import json_util
import redis
import json
from functools import reduce

# Connect to MongoDB
client = pymongo.MongoClient("mongodb://mongodb:27017/")
mongo_db = client["tpch"]

# Fetch data from MongoDB
orders_collection = mongo_db["orders"]
lineitem_collection = mongo_db["lineitem"]

orders_query = {}
orders_projection = {"_id": False, "O_ORDERKEY": True, "O_CUSTKEY": True, "O_ORDERDATE": True, "O_TOTALPRICE": True}
orders_df = pd.DataFrame(list(orders_collection.find(orders_query, orders_projection)))

lineitem_query = {}
lineitem_projection = {"_id": False, "L_ORDERKEY": True, "L_QUANTITY": True}
lineitem_df = pd.DataFrame(list(lineitem_collection.find(lineitem_query, lineitem_projection)))

# Filter lineitem for SUM(L_QUANTITY) > 300
lineitem_grouped = lineitem_df.groupby('L_ORDERKEY').filter(lambda x: x['L_QUANTITY'].sum() > 300)

# Connect to Redis
r = redis.StrictRedis(host='redis', port=6379, db=0, decode_responses=True)
customer_json = r.get('customer')
customer_df = pd.read_json(customer_json, orient='records')

# Merge data frames
lineitem_filtered_orders = lineitem_grouped['L_ORDERKEY'].drop_duplicates().to_frame()
orders_filtered = orders_df.merge(lineitem_filtered_orders, left_on='O_ORDERKEY', right_on='L_ORDERKEY')
customer_orders = orders_filtered.merge(customer_df, left_on='O_CUSTKEY', right_on='C_CUSTKEY')

# Compute sum of L_QUANTITY over O_ORDERKEY
lineitem_summary = lineitem_grouped.groupby('L_ORDERKEY').agg({'L_QUANTITY': 'sum'}).reset_index()

# Final merge for query result
final_result = customer_orders.merge(lineitem_summary, left_on='O_ORDERKEY', right_on='L_ORDERKEY')

# Group by fields specified in query and sum L_QUANTITY for each group
final_result_grouped = final_result.groupby(
    ['C_NAME', 'C_CUSTKEY', 'O_ORDERKEY', 'O_ORDERDATE', 'O_TOTALPRICE']).agg({'L_QUANTITY': 'sum'}).reset_index()

# Sort by O_TOTALPRICE DESC, O_ORDERDATE
final_result_grouped_sorted = final_result_grouped.sort_values(
    by=['O_TOTALPRICE', 'O_ORDERDATE'], ascending=[False, True])

# Write result to csv file
final_result_grouped_sorted.to_csv('query_output.csv', index=False)
