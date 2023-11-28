import pymongo
import pandas as pd
from datetime import datetime
from direct_redis import DirectRedis
import csv

# Connect to MongoDB for 'part' table
mongo_client = pymongo.MongoClient("mongodb://mongodb:27017/")
mongo_db = mongo_client["tpch"]
parts_collection = mongo_db["part"]

# Query for picking "PROMO" parts from 'part' table
promo_parts = list(parts_collection.find(
    {"P_TYPE": {"$regex": "^PROMO"}},
    {"_id": 0, "P_PARTKEY": 1}
))

# Transform the list of dictionaries to DataFrame
promo_parts_df = pd.DataFrame(promo_parts)

# Connect to Redis for 'lineitem' table
redis_client = DirectRedis(host='redis', port=6379, db=0)

# Retrieve 'lineitem' table and load into DataFrame
lineitem_df = pd.read_json(redis_client.get('lineitem'), orient='records')

# Filtering the 'lineitem' table based on the given dates
start_date = datetime.strptime("1995-09-01", "%Y-%m-%d")
end_date = datetime.strptime("1995-09-30", "%Y-%m-%d")

lineitem_filtered_df = lineitem_df[
    (pd.to_datetime(lineitem_df['L_SHIPDATE']) >= start_date) &
    (pd.to_datetime(lineitem_df['L_SHIPDATE']) <= end_date)
]

# Merge the two DataFrames on part key
merged_df = pd.merge(lineitem_filtered_df, promo_parts_df, how='inner', left_on='L_PARTKEY', right_on='P_PARTKEY')

# Adjust extended price for discount and calculate sum for promo and total
merged_df['ADJUSTED_EXTENDEDPRICE'] = merged_df['L_EXTENDEDPRICE'] * (1 - merged_df['L_DISCOUNT'])
promo_revenue = merged_df['ADJUSTED_EXTENDEDPRICE'].sum()
total_revenue = lineitem_filtered_df['L_EXTENDEDPRICE'].sum()

# Calculate promotional revenue as a percentage of total revenue
promo_revenue_percentage = (promo_revenue / total_revenue) * 100

# Write result to CSV file
with open('query_output.csv', mode='w', newline='') as file:
    writer = csv.writer(file)
    writer.writerow(['Promo Revenue Percentage'])
    writer.writerow([promo_revenue_percentage])
