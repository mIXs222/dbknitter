# import necessary libraries
import pymongo
import pandas as pd
from direct_redis import DirectRedis
from datetime import datetime
import csv

# Connect to MongoDB
client = pymongo.MongoClient("mongodb", 27017)
mongodb = client.tpch
lineitem_collection = mongodb.lineitem

# Get lineitem data from MongoDB within the timeframe
start_date = datetime(1995, 9, 1)
end_date = datetime(1995, 9, 30)

# Query MongoDB for lineitem data in the specified timeframe
lineitem_data = list(lineitem_collection.find({
    'L_SHIPDATE': {'$gte': start_date, '$lte': end_date}},
    {'_id': 0}
))

# Convert to pandas DataFrame
lineitem_df = pd.DataFrame(lineitem_data)

# Connect to Redis
redis_client = DirectRedis(host='redis', port=6379, db=0)

# Get part data from Redis
part_keys = redis_client.keys('part*')
part_rows = [eval(redis_client.get(k)) for k in part_keys]
part_df = pd.DataFrame(part_rows)

# Keep only parts with type starting with 'PROMO'
promo_parts = part_df[part_df['P_TYPE'].str.startswith('PROMO')]

# Calculate promotional revenue and total revenue
lineitem_df['L_DISCOUNTED_PRICE'] = lineitem_df['L_EXTENDEDPRICE'] * (1 - lineitem_df['L_DISCOUNT'])

# Merge lineitem data with promotional parts data
promo_revenue_df = lineitem_df.merge(promo_parts, how='inner', left_on='L_PARTKEY', right_on='P_PARTKEY')

# Calculate sums
promotional_revenue = promo_revenue_df['L_DISCOUNTED_PRICE'].sum()
total_revenue = lineitem_df['L_DISCOUNTED_PRICE'].sum()

# Calculate percentage
percentage_promo_revenue = (promotional_revenue / total_revenue) * 100 if total_revenue else 0

# Prepare the output
output = [{
    'Promotional_Revenue': promotional_revenue,
    'Total_Revenue': total_revenue,
    'Percentage_Promo_Revenue': percentage_promo_revenue
}]

# Write output to CSV file
with open('query_output.csv', mode='w') as f:
    writer = csv.DictWriter(f, fieldnames=output[0].keys())
    writer.writeheader()
    for data in output:
        writer.writerow(data)

# Close connection
client.close()
