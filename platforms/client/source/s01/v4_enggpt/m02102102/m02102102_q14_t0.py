from pymongo import MongoClient
import pandas as pd
from datetime import datetime
import direct_redis

# MongoDB Connection Setup
mongodb_client = MongoClient('mongodb', 27017)
mongodb_db = mongodb_client['tpch']

# Redis Connection Setup
redis_client = direct_redis.DirectRedis(host='redis', port=6379, db=0)

# Load 'part' table from MongoDB
part_collection = mongodb_db['part']
part_query = {'P_TYPE': {'$regex': '^PROMO'}}
parts = pd.DataFrame(list(part_collection.find(part_query)))

# Load 'lineitem' table from Redis
lineitem_key = 'lineitem'
lineitem_df = pd.read_msgpack(redis_client.get(lineitem_key))

# Convert string dates to datetime objects in 'lineitem' DataFrame
lineitem_df['L_SHIPDATE'] = pd.to_datetime(lineitem_df['L_SHIPDATE'])

# Filter the line items for the specific timeframe
start_date = datetime(1995, 9, 1)
end_date = datetime(1995, 9, 30)
filtered_lineitems = lineitem_df[(lineitem_df['L_SHIPDATE'] >= start_date) &
                                 (lineitem_df['L_SHIPDATE'] <= end_date)]

# Join the parts and line items on 'P_PARTKEY' and 'L_PARTKEY'
joined_data = filtered_lineitems.merge(parts, left_on='L_PARTKEY', right_on='P_PARTKEY')

# Calculate the sum of extended prices for promo parts, adjusted by discounts
promo_revenue = (joined_data['L_EXTENDEDPRICE'] *
                 (1 - joined_data['L_DISCOUNT'])).sum()

# Calculate the total sum of extended prices for all line items adjusted by discount
total_revenue = (filtered_lineitems['L_EXTENDEDPRICE'] *
                 (1 - filtered_lineitems['L_DISCOUNT'])).sum()

# Calculate the promotional revenue percentage
promo_revenue_percentage = (promo_revenue / total_revenue) * 100 if total_revenue else 0

# Create a DataFrame to output the result
result = pd.DataFrame({
    'Promotional Revenue (%)': [promo_revenue_percentage]
})

# Write the results to 'query_output.csv'
result.to_csv('query_output.csv', index=False)

# Close the MongoDB connection
mongodb_client.close()
