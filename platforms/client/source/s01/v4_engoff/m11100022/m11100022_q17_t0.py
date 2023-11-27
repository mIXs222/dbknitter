import pandas as pd
from pymongo import MongoClient
from direct_redis import DirectRedis

# Connect to MongoDB
mongo_client = MongoClient('mongodb', 27017)
mongo_db = mongo_client['tpch']

# Connect to Redis
redis_client = DirectRedis(host='redis', port=6379, db=0)

# Fetch data from MongoDB
part_collection = mongo_db['part']
parts_df = pd.DataFrame(list(part_collection.find({"P_BRAND": "Brand#23", "P_CONTAINER": "MED BAG"})))

# Fetch data from Redis
lineitem_df = pd.read_json(redis_client.get('lineitem'))

# Filter lineitem df for relevant part keys
part_keys = parts_df['P_PARTKEY'].tolist()
filtered_lineitems = lineitem_df[lineitem_df['L_PARTKEY'].isin(part_keys)]

# Calculate average quantity for parts
avg_quantity = filtered_lineitems['L_QUANTITY'].mean()

# Calculate 20% of the average quantity
quantity_threshold = avg_quantity * 0.2

# Filter orders with small quantities
small_quantity_orders = filtered_lineitems[filtered_lineitems['L_QUANTITY'] < quantity_threshold]

# Calculate the total undiscouned revenue loss
small_quantity_orders['UNDISCOUNTED_LOSS'] = small_quantity_orders['L_QUANTITY'] * small_quantity_orders['L_EXTENDEDPRICE']
total_loss = small_quantity_orders['UNDISCOUNTED_LOSS'].sum()

# Considering 7 years of data, calculate the average yearly loss
average_yearly_loss = total_loss / 7

# Write the result to query_output.csv
result_df = pd.DataFrame({'Average_Yearly_Loss': [average_yearly_loss]})
result_df.to_csv('query_output.csv', index=False)
