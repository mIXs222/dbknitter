import pymongo
import pandas as pd
import direct_redis

# Connect to MongoDB
mongo_client = pymongo.MongoClient("mongodb", 27017)
mongo_db = mongo_client["tpch"]
parts_collection = mongo_db["part"]

# Connect to Redis
redis_client = direct_redis.DirectRedis(host="redis", port=6379, db=0)

# Get the relevant parts from MongoDB
part_query = {"P_BRAND": "BRAND#23", "P_CONTAINER": "MED BAG"}
part_projection = {"P_PARTKEY": 1}
relevant_parts_df = pd.DataFrame(list(parts_collection.find(part_query, part_projection)))

# Get lineitem data from Redis as a DataFrame
lineitem_df = pd.read_msgpack(redis_client.get('lineitem'))

# Join on partkey to filter relevant lineitems
combined_df = lineitem_df[lineitem_df['L_PARTKEY'].isin(relevant_parts_df['P_PARTKEY'])]

# Calculate the average quantity of small quantity orders
avg_quantity = combined_df['L_QUANTITY'].mean()
small_quantity_orders = combined_df[combined_df['L_QUANTITY'] < avg_quantity * 0.2]

# Calculate average yearly gross loss
small_quantity_orders['GROSS_LOSS'] = small_quantity_orders['L_EXTENDEDPRICE'] * (1 - small_quantity_orders['L_DISCOUNT'])
average_yearly_loss = small_quantity_orders['GROSS_LOSS'].sum() / 7

# Write the output
output_df = pd.DataFrame({'Average_Yearly_Loss': [average_yearly_loss]})
output_df.to_csv('query_output.csv', index=False)
