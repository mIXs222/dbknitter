import pandas as pd
import pymongo
from bson import json_util
import direct_redis
import json

# Function to write DataFrame to CSV
def write_to_csv(df, filename):
    df.to_csv(filename, index=False)

# Connect to MongoDB
client = pymongo.MongoClient('mongodb', 27017)
mongodb = client['tpch']
part_collection = mongodb['part']

# Get parts data from MongoDB
part_filter = {'P_BRAND': 'Brand#23', 'P_CONTAINER': 'MED BAG'}
parts_data = part_collection.find(part_filter, {'_id': 0})
parts_df = pd.DataFrame(list(parts_data))

# Connect to Redis
r = direct_redis.DirectRedis(host='redis', port=6379, db=0)

# Get lineitem data from Redis
lineitem_data = r.get('lineitem')
lineitem_df = pd.read_json(lineitem_data, orient='records')

# Process the data
# Compute average quantity for each part
avg_qty_per_part = lineitem_df.groupby('L_PARTKEY')['L_QUANTITY'].mean().reset_index()
avg_qty_per_part.rename(columns={'L_QUANTITY': 'AVG_QTY'}, inplace=True)

# Merge lineitem dataframe with the average quantities
lineitem_avg_qty_df = lineitem_df.merge(avg_qty_per_part, left_on='L_PARTKEY', right_on='L_PARTKEY')

# Filter lineitems where the quantity is less than 20% of the average quantity of the same part
filtered_lineitems = lineitem_avg_qty_df[lineitem_avg_qty_df['L_QUANTITY'] < lineitem_avg_qty_df['AVG_QTY'] * 0.2]

# Merge filtered lineitems with parts data
merged_data = filtered_lineitems.merge(parts_df, how='inner', left_on='L_PARTKEY', right_on='P_PARTKEY')

# Calculate the sum of L_EXTENDEDPRICE and then take the average over the years (assuming 7 years)
total_extended_price = merged_data['L_EXTENDEDPRICE'].sum()
average_yearly_extended_price = total_extended_price / 7.0

# Generate the final result
result_df = pd.DataFrame({'AverageYearlyExtendedPrice': [average_yearly_extended_price]})

# Save result to CSV
write_to_csv(result_df, 'query_output.csv')
