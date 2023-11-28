import pymongo
import pandas as pd
from direct_redis import DirectRedis

# Connect to MongoDB
client = pymongo.MongoClient('mongodb', 27017)
db = client['tpch']
lineitem_collection = db['lineitem']
lineitems_df = pd.DataFrame(list(lineitem_collection.find()))

# Connect to Redis
redis_client = DirectRedis(host='redis', port=6379, db=0)
part_df = pd.read_msgpack(redis_client.get('part'))

# Filter parts with brand 'Brand#23' and 'MED BAG'
filtered_parts_df = part_df[(part_df['P_BRAND'] == 'Brand#23') &
                            (part_df['P_CONTAINER'] == 'MED BAG')]

# Calculate the average quantity for the filtered parts
avg_quantity_df = lineitems_df.groupby('L_PARTKEY')['L_QUANTITY'].mean().reset_index()
avg_quantity_df.columns = ['L_PARTKEY', 'AVG_QUANTITY']

# Merge with filtered parts on part key
merged_df = pd.merge(left=lineitems_df, right=filtered_parts_df, how='inner', left_on='L_PARTKEY', right_on='P_PARTKEY')

# Merge with average quantity on part key
merged_df = pd.merge(left=merged_df, right=avg_quantity_df, on='L_PARTKEY')

# Filter line items where the quantity is less than 20% of the AVG_QUANTITY
final_df = merged_df[merged_df['L_QUANTITY'] < (0.2 * merged_df['AVG_QUANTITY'])]

# Calculate the sum of extended prices
total_extended_price = final_df['L_EXTENDEDPRICE'].sum()

# Calculate the average yearly extended price
average_yearly_extended_price = total_extended_price / 7.0

# Output the result to a CSV file
result_df = pd.DataFrame({'AverageYearlyExtendedPrice': [average_yearly_extended_price]})
result_df.to_csv('query_output.csv', index=False)
