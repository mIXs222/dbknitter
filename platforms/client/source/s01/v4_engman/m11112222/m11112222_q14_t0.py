# promotion_effect_query.py

import pymongo
from bson.json_util import loads
from datetime import datetime
import pandas as pd
import direct_redis
import csv

# Connect to MongoDB
client_mongo = pymongo.MongoClient("mongodb://mongodb:27017/")
db_mongo = client_mongo["tpch"]

# Get the 'part' collection from MongoDB
part_col = db_mongo["part"]

# Fetch the data from 'part' collection for promotional parts
promotional_parts = list(part_col.find({}, {"_id": 0}))

# Convert to DataFrame
df_parts = pd.DataFrame(promotional_parts)

# Connect to Redis
client_redis = direct_redis.DirectRedis(host='redis', port=6379, db=0)

# Get the 'lineitem' data from Redis
lineitem_data = client_redis.get('lineitem')

# Convert JSON string to pandas DataFrame
df_lineitem = pd.read_json(loads(lineitem_data))

# Filter the lineitem DataFrame for the specified date range
start_date = datetime(1995, 9, 1)
end_date = datetime(1995, 10, 1)
df_lineitem = df_lineitem[(df_lineitem['L_SHIPDATE'] >= start_date) & (df_lineitem['L_SHIPDATE'] < end_date)]

# Join parts and lineitems DataFrames on P_PARTKEY and L_PARTKEY
df_result = pd.merge(df_lineitem, df_parts, left_on='L_PARTKEY', right_on='P_PARTKEY')

# Calculate the revenue
df_result['revenue'] = df_result['L_EXTENDEDPRICE'] * (1 - df_result['L_DISCOUNT'])

# Calculate the total revenue
total_revenue = df_result['revenue'].sum()

# Calculate the revenue from promotional parts
promotional_revenue = df_result[df_result['P_TYPE'].str.contains('PROMO')]['revenue'].sum()

# Calculate the promotion effect percentage
promotion_effect_percentage = (promotional_revenue / total_revenue) * 100 if total_revenue else 0

# Prepare the output
output_data = {'promotion_effect_percentage': promotion_effect_percentage}

# Write to csv file
with open('query_output.csv', mode='w', newline='') as file:
    writer = csv.writer(file)
    writer.writerow(output_data.keys())
    writer.writerow(output_data.values())

print(f"The promotion effect percentage for the given date range is: {promotion_effect_percentage}%")
