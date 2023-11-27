# query_execute.py
import pymongo
import pandas as pd
from direct_redis import DirectRedis

# Connect to MongoDB
client = pymongo.MongoClient("mongodb://mongodb:27017/")
mongodb = client["tpch"]
part_collection = mongodb["part"]

# Connect to Redis
redis = DirectRedis(host='redis', port=6379, db=0)

# Query MongoDB for parts with brand 'Brand#23' and container 'MED BAG'
part_query = {"P_BRAND": "Brand#23", "P_CONTAINER": "MED BAG"}
parts_df = pd.DataFrame(list(part_collection.find(part_query, {'_id': 0})))

# Query Redis for lineitems dataframe
lineitem_df = pd.read_json(redis.get('lineitem'), orient='records')

# Merge MongoDB and Redis dataframes
merged_df = lineitem_df.merge(parts_df, left_on='L_PARTKEY', right_on='P_PARTKEY')

# Calculate the average lineitem quantity
average_quantity = merged_df['L_QUANTITY'].mean()

# Calculate the total revenue for lineitems where quantity is less than 20% of the average
small_quantity_revenue = merged_df.loc[merged_df['L_QUANTITY'] < average_quantity * 0.2, 'L_EXTENDEDPRICE'].sum()

# Number of years in the database (7 years)
years_in_db = 7

# Calculate average yearly loss in revenue
average_yearly_loss = small_quantity_revenue / years_in_db

# Output the result to a CSV file
output_df = pd.DataFrame({'Average_Yearly_Loss': [average_yearly_loss]})
output_df.to_csv('query_output.csv', index=False)
