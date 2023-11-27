# query.py
from pymongo import MongoClient
import pandas as pd
import direct_redis

# Function to connect to MongoDB
def get_mongo_client(host, port):
    return MongoClient(host, port)

# Connecting to MongoDB
mongo_client = get_mongo_client('mongodb', 27017)
mongodb = mongo_client['tpch']

# Querying the part collection
part_query = {"$and": [{"P_BRAND": "Brand#23"}, {"P_CONTAINER": "MED BAG"}]}
part_projection = {"P_PARTKEY": 1, "_id": 0}  # Include only the P_PARTKEY field and exclude _id
part_df = pd.DataFrame(list(mongodb.part.find(part_query, part_projection)))

# Connect to Redis
r = direct_redis.DirectRedis(host='redis', port=6379, db=0)

# Get lineitem data from Redis into a DataFrame
lineitem_df = pd.read_json(r.get('lineitem'))

# Filter lineitem DataFrame based on the received part keys from MongoDB
lineitem_filtered = lineitem_df[lineitem_df['L_PARTKEY'].isin(part_df['P_PARTKEY'])]

# Calculate the average quantity for parts
avg_qty = lineitem_filtered.groupby('L_PARTKEY')['L_QUANTITY'].mean().reset_index()
avg_qty['AVG_QUANTITY'] = avg_qty['L_QUANTITY'] * 0.2
avg_qty = avg_qty[['L_PARTKEY', 'AVG_QUANTITY']]

# Merge data frames and filter lineitem DataFrame further based on quantity condition
conditioned_lineitem_df = pd.merge(lineitem_filtered, avg_qty, left_on='L_PARTKEY', right_on='L_PARTKEY')
final_lineitem = conditioned_lineitem_df[conditioned_lineitem_df['L_QUANTITY'] < conditioned_lineitem_df['AVG_QUANTITY']]

# Compute the SUM(L_EXTENDEDPRICE) / 7.0
result = pd.DataFrame({'AVG_YEARLY': [(final_lineitem['L_EXTENDEDPRICE'].sum()) / 7.0]})

# Write results to CSV
result.to_csv('query_output.csv', index=False)
