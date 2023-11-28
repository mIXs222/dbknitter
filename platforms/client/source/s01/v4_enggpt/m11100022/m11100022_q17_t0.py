import pandas as pd
from pymongo import MongoClient
from direct_redis import DirectRedis

# Establish connection to MongoDB for the 'part' table
mongo_client = MongoClient(host='mongodb', port=27017)
mongo_db = mongo_client['tpch']
part_collection = mongo_db['part']

# Query for parts with brand 'Brand#23' and container type 'MED BAG'
part_query = {'P_BRAND': 'Brand#23', 'P_CONTAINER': 'MED BAG'}
parts_df = pd.DataFrame(list(part_collection.find(part_query, projection={'_id': False})))

# Establish connection to Redis for 'lineitem' table
redis_client = DirectRedis(host='redis', port=6379, db=0)
lineitem_data = redis_client.get('lineitem')
lineitem_df = pd.read_json(lineitem_data)

# Calculate average quantity per part
avg_qty_per_part = lineitem_df.groupby('L_PARTKEY')['L_QUANTITY'].mean().reset_index()
avg_qty_per_part.rename(columns={'L_QUANTITY': 'AVG_QUANTITY'}, inplace=True)

# Merge line items with average quantity
lineitem_df = lineitem_df.merge(avg_qty_per_part, how='left', left_on='L_PARTKEY', right_on='L_PARTKEY')
lineitem_df['QUANTITY_THRESHOLD'] = lineitem_df['AVG_QUANTITY'] * 0.2

# Filter line items according to quantity
filtered_lineitems = lineitem_df[lineitem_df['L_QUANTITY'] < lineitem_df['QUANTITY_THRESHOLD']]

# Merge parts with the filtered line items to get the final result
final_df = parts_df.merge(filtered_lineitems, how='inner', left_on='P_PARTKEY', right_on='L_PARTKEY')

# Calculate average yearly extended price for the filtered line items
final_df['YEARLY_EXTENDEDPRICE'] = final_df['L_EXTENDEDPRICE'].sum() / 7.0
result = final_df[['YEARLY_EXTENDEDPRICE']].drop_duplicates()

# Write result to CSV
result.to_csv('query_output.csv', index=False)
