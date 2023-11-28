import pymongo
import pandas as pd
from bson.json_util import dumps
import direct_redis
import json

# Connect to MongoDB
mongo_client = pymongo.MongoClient('mongodb://mongodb:27017/')
mongo_db = mongo_client['tpch']
mongo_collection = mongo_db['lineitem']

# Connect to Redis
redis_conn = direct_redis.DirectRedis(host='redis', port=6379, db=0)

# Read 'part' table from Redis and convert to pandas DataFrame
part_data = json.loads(redis_conn.get('part'))
part_df = pd.DataFrame(part_data)

# Filter 'part' DataFrame for 'Brand#23' and 'MED BAG'
filtered_parts = part_df[(part_df['P_BRAND'] == 'Brand#23') & (part_df['P_CONTAINER'] == 'MED BAG')]

# Retrieve 'lineitem' table in MongoDB in its entirety
lineitem_data = mongo_collection.find({}, {'_id': False})
lineitem_df = pd.DataFrame(list(lineitem_data))

# Image we have part and lineitem data loaded appropriately
# Create a DataFrame for average quantities
average_quantity_df = lineitem_df.groupby('L_PARTKEY')['L_QUANTITY'].mean().reset_index()
average_quantity_df.columns = ['L_PARTKEY', 'AVG_QUANTITY']

# Merge filtered_parts with average_quantity_df to get average quantity
filtered_parts = filtered_parts.merge(average_quantity_df, left_on='P_PARTKEY', right_on='L_PARTKEY')

# Keep only parts whose line item quantity is less than 20% of the average quantity of the same part
filtered_lineitem_df = lineitem_df.merge(filtered_parts, left_on='L_PARTKEY', right_on='P_PARTKEY')
filtered_lineitem_df = filtered_lineitem_df[filtered_lineitem_df['L_QUANTITY'] < 0.2 * filtered_lineitem_df['AVG_QUANTITY']]

# Calculate the average yearly extended price
filtered_lineitem_df['YEARLY_EXTENDED_PRICE'] = filtered_lineitem_df['L_EXTENDEDPRICE'] / 7.0

# Group by part key and calculate the average
results = filtered_lineitem_df.groupby('L_PARTKEY')['YEARLY_EXTENDED_PRICE'].mean().reset_index()

# Write the result to csv file
results.to_csv('query_output.csv', index=False)
