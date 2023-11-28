import pymongo
import pandas as pd
from datetime import datetime
import csv

# MongoDB Connection
mongo_client = pymongo.MongoClient("mongodb", 27017)
mongo_db = mongo_client['tpch']
part_collection = mongo_db['part']
parts_df = pd.DataFrame(list(part_collection.find()))

# Redis Connection (assuming direct_redis.DirectRedis is a placeholder for correct library)
# Replace following line with correct import if direct_redis is a module/library
import redis as direct_redis
redis_client = direct_redis.DirectRedis(host='redis', port=6379, db=0)
lineitem_df = redis_client.get('lineitem')
lineitem_df = pd.read_json(lineitem_df)

# Convert strings to datetime
lineitem_df['L_SHIPDATE'] = pd.to_datetime(lineitem_df['L_SHIPDATE'])

# Filter date range
start_date = datetime(1995, 9, 1)
end_date = datetime(1995, 9, 30)
filtered_lineitems = lineitem_df[
    (lineitem_df['L_SHIPDATE'] >= start_date) &
    (lineitem_df['L_SHIPDATE'] <= end_date)
]

# Filter parts with 'PROMO' as prefix in P_TYPE
promo_parts = parts_df[parts_df['P_TYPE'].str.startswith('PROMO')]
promo_parts_keys = promo_parts['P_PARTKEY'].tolist()

# Filter lineitems with promo parts
promo_lineitems = filtered_lineitems[
    filtered_lineitems['L_PARTKEY'].isin(promo_parts_keys)
]

# Calculate total and promotional sums
total_revenue = (filtered_lineitems['L_EXTENDEDPRICE'] * (1 - filtered_lineitems['L_DISCOUNT'])).sum()
promo_revenue = (promo_lineitems['L_EXTENDEDPRICE'] * (1 - promo_lineitems['L_DISCOUNT'])).sum()

# Calculate promotional revenue percentage
promo_percentage = (promo_revenue / total_revenue) * 100 if total_revenue else 0

# Write to CSV file
with open('query_output.csv', 'w', newline='') as file:
    writer = csv.writer(file)
    writer.writerow(['Promotional_Revenue_Percentage'])
    writer.writerow([promo_percentage])

# Close database connections
mongo_client.close()
redis_client.close()
