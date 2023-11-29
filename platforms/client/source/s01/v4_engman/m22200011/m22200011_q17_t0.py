import pymongo
import pandas as pd
from direct_redis import DirectRedis

# MongoDB connection
mongo_client = pymongo.MongoClient("mongodb://mongodb:27017/")
mongo_db = mongo_client["tpch"]
lineitem_collection = mongo_db["lineitem"]

# Redis connection (assuming direct_redis has equivalent interface as redis-py)
redis_client = DirectRedis(host='redis', port=6379, db=0)

# Retrieve 'part' table from Redis and store as Pandas DataFrame
part_dict = redis_client.get('part')
part_df = pd.DataFrame(part_dict)

# Filter out the parts with BRAND#23 and MED BAG container
filtered_parts = part_df[(part_df['P_BRAND'] == 'BRAND#23') & (part_df['P_CONTAINER'] == 'MED BAG')]

# Retrieve 'lineitem' data as a list of dictionaries
lineitem_dicts = list(lineitem_collection.find())
lineitem_df = pd.DataFrame(lineitem_dicts)

# Merge the lineitem DataFrame with filtered parts DataFrame on P_PARTKEY and L_PARTKEY
merged_df = pd.merge(lineitem_df, filtered_parts, left_on='L_PARTKEY', right_on='P_PARTKEY')

# Calculate the average quantity of those parts
average_quantity = merged_df['L_QUANTITY'].mean()

# Get lineitems with quantity less than 20% of average
small_quantity_orders = merged_df[merged_df['L_QUANTITY'] < (0.2 * average_quantity)]

# Calculate yearly gross loss: (Extended Price * Quantity) / Number of years in data
num_years = 7  # assuming 7 full years in the dataset
small_quantity_orders['YearlyGrossLoss'] = small_quantity_orders['L_EXTENDEDPRICE'] * small_quantity_orders['L_QUANTITY'] / num_years

# Compute the average yearly loss and output to csv
average_yearly_loss = pd.DataFrame({'AverageYearlyLoss': [small_quantity_orders['YearlyGrossLoss'].mean()]})
average_yearly_loss.to_csv('query_output.csv', index=False)
