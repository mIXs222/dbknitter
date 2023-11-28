import pymongo
import pandas as pd

# Connection to MongoDB
mongo_client = pymongo.MongoClient("mongodb://mongodb:27017/")
mongo_db = mongo_client["tpch"]
part_collection = mongo_db["part"]

# Retrieve parts with brand 'Brand#23' and container type 'MED BAG'
part_criteria = {"P_BRAND": "Brand#23", "P_CONTAINER": "MED BAG"}
parts_df = pd.DataFrame(list(part_collection.find(part_criteria)))

# Preparing Redis connection and fetching lineitem data
from direct_redis import DirectRedis
redis_client = DirectRedis(host='redis', port=6379, db=0)

# Retrieving 'lineitem' table from Redis and loading it into a DataFrame
lineitem_table = redis_client.get('lineitem')
lineitem_df = pd.read_json(lineitem_table, orient='records')

# Merging 'lineitem' and 'part' data
merged_df = lineitem_df.merge(parts_df, how='inner', left_on='L_PARTKEY', right_on='P_PARTKEY')

# Calculate average quantity for each part
average_qty_per_part = merged_df.groupby('L_PARTKEY')['L_QUANTITY'].mean().reset_index()
average_qty_per_part.rename(columns={'L_QUANTITY': 'AVG_QUANTITY'}, inplace=True)

# Merge with the average quantity information and apply the quantity filter
merged_df = merged_df.merge(average_qty_per_part, on='L_PARTKEY')
filtered_df = merged_df[merged_df['L_QUANTITY'] < 0.2 * merged_df['AVG_QUANTITY']]

# Calculating the average yearly extended price
filtered_df['AVG_YEARLY_EXTENDEDPRICE'] = filtered_df['L_EXTENDEDPRICE'] / 7.0
result_df = filtered_df.groupby(['P_BRAND', 'P_CONTAINER'])['AVG_YEARLY_EXTENDEDPRICE'].mean().reset_index()

# Write final result to CSV
result_df.to_csv('query_output.csv', index=False)
