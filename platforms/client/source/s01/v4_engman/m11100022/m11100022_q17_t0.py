# This is mongo_redis_query.py
import pymongo
import pandas as pd
from direct_redis import DirectRedis
import csv

# Connect to MongoDB
client = pymongo.MongoClient("mongodb", 27017)
mongo_db = client['tpch']
part_collection = mongo_db['part']

# Retrieve parts with BRAND#23 and MED BAG from MongoDB
query_parts = {'P_BRAND': 'BRAND#23', 'P_CONTAINER': 'MED BAG'}
parts_df = pd.DataFrame(list(part_collection.find(query_parts, {'_id': 0})))

# Connect to Redis
redis_client = DirectRedis(host='redis', port=6379, db=0)

# Retrieve lineitem table from Redis and convert to DataFrame
lineitem_raw = redis_client.get('lineitem')
lineitem_df = pd.read_json(lineitem_raw, orient='records')

# Merge parts and lineitem DataFrames on P_PARTKEY and L_PARTKEY
merged_df = pd.merge(parts_df, lineitem_df, left_on='P_PARTKEY', right_on='L_PARTKEY')

# Filter orders with quantity less than 20% of average quantity
average_quantity = merged_df['L_QUANTITY'].mean()
quantity_threshold = 0.2 * average_quantity
small_orders_df = merged_df[merged_df['L_QUANTITY'] < quantity_threshold]

# Calculate the average yearly loss
total_loss = small_orders_df['L_EXTENDEDPRICE'].sum()
num_years = 7
average_yearly_loss = total_loss / num_years

# Write the output to a CSV file
with open('query_output.csv', 'w', newline='') as csvfile:
    csvwriter = csv.writer(csvfile)
    csvwriter.writerow(['Average Yearly Loss'])
    csvwriter.writerow([average_yearly_loss])

print("The average yearly loss has been successfully written to query_output.csv")
