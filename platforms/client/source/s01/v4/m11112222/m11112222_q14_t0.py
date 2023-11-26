# query.py
from pymongo import MongoClient
import redis
import pandas as pd
from direct_redis import DirectRedis

# Connect to MongoDB
client = MongoClient('mongodb', 27017)
db = client['tpch']
part_collection = db['part']

# Fetch part data
part_query = {"P_TYPE": {"$regex": "^PROMO"}}
projection = {
    "_id": 0, 
    "P_PARTKEY": 1,
    "P_NAME": 0, "P_MFGR": 0, "P_BRAND": 0, 
    "P_TYPE": 0, "P_SIZE": 0, "P_CONTAINER": 0, 
    "P_RETAILPRICE": 0, "P_COMMENT": 0
}
part_data = list(part_collection.find(part_query, projection))
part_df = pd.DataFrame(part_data)

# Connect to Redis
r = DirectRedis(host='redis', port=6379, db=0)

# Fetch lineitem data from Redis
lineitem_df = pd.read_json(r.get('lineitem'))
lineitem_df['L_SHIPDATE'] = pd.to_datetime(lineitem_df['L_SHIPDATE'])

# Filter the lineitems for the date range
filtered_lineitem_df = lineitem_df[
    (lineitem_df['L_SHIPDATE'] >= '1995-09-01') &
    (lineitem_df['L_SHIPDATE'] < '1995-10-01')
]

# Merge dataframes on partkey
merged_df = pd.merge(filtered_lineitem_df, part_df, left_on="L_PARTKEY", right_on="P_PARTKEY")

# Compute PROMO_REVENUE
revenue = merged_df['L_EXTENDEDPRICE'] * (1 - merged_df['L_DISCOUNT'])
promo_revenue = (revenue * (merged_df['P_TYPE'].str.startswith("PROMO", na=False))).sum()
total_revenue = revenue.sum()
result = 100.00 * promo_revenue / total_revenue if total_revenue else 0

# Create result DataFrame
result_df = pd.DataFrame([{'PROMO_REVENUE': result}])

# Write to a CSV file
result_df.to_csv('query_output.csv', index=False)
