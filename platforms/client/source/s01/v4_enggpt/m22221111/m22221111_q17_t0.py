import pymongo
import pandas as pd
import direct_redis
import csv

# MongoDB connection
client = pymongo.MongoClient("mongodb://mongodb:27017/")
mongo_db = client["tpch"]
lineitem_collection = mongo_db["lineitem"]

# Retrieve lineitem data
lineitem_cursor = lineitem_collection.find(
    {}, {'L_PARTKEY': 1, 'L_EXTENDEDPRICE': 1, 'L_QUANTITY': 1}
)

# Convert cursor to DataFrame
lineitem_df = pd.DataFrame(list(lineitem_cursor))

# Redis connection
redis_client = direct_redis.DirectRedis(host='redis', port=6379, db=0)

# Retrieve part data as DataFrame
part_df = pd.read_json(redis_client.get('part'))

# Filter parts by brand and container
filtered_parts = part_df[(part_df['P_BRAND'] == 'Brand#23') & (part_df['P_CONTAINER'] == 'MED BAG')]

# Calculate average quantities for each part
avg_quantities = lineitem_df.groupby('L_PARTKEY')['L_QUANTITY'].mean().reset_index()

# Calculate 20% of the average quantities
avg_quantities['avg_20_percent'] = avg_quantities['L_QUANTITY'] * 0.2

# Join lineitem with filtered parts on part key
result_df = pd.merge(lineitem_df, filtered_parts, left_on='L_PARTKEY', right_on='P_PARTKEY')

# Join lineitem and parts with average quantities
result_df = pd.merge(result_df, avg_quantities, left_on='L_PARTKEY', right_on='L_PARTKEY')

# Filter lineitem based on less than 20% of the average quantity condition
result_df = result_df[result_df['L_QUANTITY'] < result_df['avg_20_percent']]

# Calculate yearly average extended price
result_df['avg_yearly_ext_price'] = result_df['L_EXTENDEDPRICE'] / 7.0

# Create final output DataFrame
output_df = result_df[['L_PARTKEY', 'L_EXTENDEDPRICE', 'avg_yearly_ext_price']]

# Write the result to a CSV file
output_df.to_csv('query_output.csv', index=False, quoting=csv.QUOTE_MINIMAL)
