from pymongo import MongoClient
import pandas as pd
from datetime import datetime
from direct_redis import DirectRedis

# MongoDB connection
client = MongoClient('mongodb', 27017)
db = client['tpch']
parts_collection = db['part']

# Fetch parts data
parts_df = pd.DataFrame(list(parts_collection.find(
    {"P_NAME": {"$regex": ".*PROMO.*"}},
    {"_id": 0, "P_PARTKEY": 1}
)))

# Redis connection
r = DirectRedis(host='redis', port=6379, db=0)
lineitem_df = pd.DataFrame(r.get('lineitem'))

# Convert data types for calculation
lineitem_df['L_PARTKEY'] = lineitem_df['L_PARTKEY'].astype(int)
lineitem_df['L_EXTENDEDPRICE'] = lineitem_df['L_EXTENDEDPRICE'].astype(float)
lineitem_df['L_DISCOUNT'] = lineitem_df['L_DISCOUNT'].astype(float)
lineitem_df['L_SHIPDATE'] = pd.to_datetime(lineitem_df['L_SHIPDATE'])

# Filter date range for line items shipped
start_date = datetime(1995, 9, 1)
end_date = datetime(1995, 10, 1)
filtered_lineitems = lineitem_df[
    (lineitem_df['L_SHIPDATE'] >= start_date) &
    (lineitem_df['L_SHIPDATE'] < end_date)
]

# Join on part key to find promotional items
promo_lineitems = pd.merge(filtered_lineitems, parts_df, left_on='L_PARTKEY', right_on='P_PARTKEY')

# Calculate revenue
promo_lineitems['REVENUE'] = promo_lineitems['L_EXTENDEDPRICE'] * (1 - promo_lineitems['L_DISCOUNT'])

# Calculate total revenue and promotional revenue
total_revenue = filtered_lineitems['L_EXTENDEDPRICE'] * (1 - filtered_lineitems['L_DISCOUNT']).sum()
promo_revenue = promo_lineitems['REVENUE'].sum()

# Calculate percentage of promotional revenue
promo_percentage = (promo_revenue / total_revenue) * 100 if total_revenue else 0

# Write to CSV file
output_df = pd.DataFrame({
    'Promotional_Revenue_Percentage': [promo_percentage]
})
output_df.to_csv('query_output.csv', index=False)
