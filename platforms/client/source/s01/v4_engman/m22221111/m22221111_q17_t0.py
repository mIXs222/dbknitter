from pymongo import MongoClient
import pandas as pd
import direct_redis

# Connect to MongoDB
mongo_client = MongoClient('mongodb', 27017)
mongodb = mongo_client['tpch']
lineitem_collection = mongodb['lineitem']

# Connect to Redis
redis_client = direct_redis.DirectRedis(host='redis', port=6379, db=0)

# Read from MongoDB
lineitem_df = pd.DataFrame(list(lineitem_collection.find({'L_RETURNFLAG': {'$in': ['BRAND#23']}, 'L_SHIPINSTRUCT': 'MED BAG'})))

# Read from Redis
part_str = redis_client.get('part')
part_df = pd.read_json(part_str, orient='records')

# Filter the relevant parts from Redis
filtered_parts = part_df[part_df['P_BRAND'] == 'BRAND#23']
filtered_parts = filtered_parts[filtered_parts['P_CONTAINER'] == 'MED BAG']

# Join the two dataframes
merged_df = pd.merge(lineitem_df, filtered_parts, left_on='L_PARTKEY', right_on='P_PARTKEY')

# Find average quantity
average_quantity = merged_df['L_QUANTITY'].mean()

# Compute average yearly gross loss
df_small_qty_orders = merged_df[merged_df['L_QUANTITY'] < (0.2 * average_quantity)]
df_small_qty_orders['GROSS_LOSS'] = df_small_qty_orders['L_EXTENDEDPRICE'] * (1 - df_small_qty_orders['L_DISCOUNT'])

# Calculate average yearly loss
result = df_small_qty_orders['GROSS_LOSS'].sum() / 7  # assuming the 7-year period is correct

# Output the result to CSV file
result_df = pd.DataFrame([{'average_yearly_loss': result}])
result_df.to_csv('query_output.csv', index=False)
