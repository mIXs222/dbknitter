# promotion_effect_query.py

from pymongo import MongoClient
import pandas as pd
from direct_redis import DirectRedis
import csv
from datetime import datetime

# Connect to the MongoDB server
mongo_client = MongoClient(host='mongodb', port=27017)
mongodb = mongo_client['tpch']

# Fetch 'lineitem' table data from MongoDB
lineitem_collection = mongodb['lineitem']
lineitem_cursor = lineitem_collection.find({
    'L_SHIPDATE': {
        '$gte': datetime(1995, 9, 1),
        '$lt': datetime(1995, 10, 1)
    }
})
lineitem_df = pd.DataFrame(list(lineitem_cursor))

# Connect to Redis server
redis_client = DirectRedis(host='redis', port=6379, db=0)

# Fetch 'part' table data from Redis
part_df = pd.read_json(redis_client.get('part'), orient='records')

# Calculate Revenue from MongoDB lineitem data
lineitem_df['REVENUE'] = lineitem_df['L_EXTENDEDPRICE'] * (1 - lineitem_df['L_DISCOUNT'])

# Join the data to find the promotional parts shipped
merged_df = pd.merge(
    lineitem_df,
    part_df,
    how='inner',
    left_on='L_PARTKEY',
    right_on='P_PARTKEY'
)

# Calculate and write the percentage revenue from promotional parts
total_revenue = lineitem_df['REVENUE'].sum()
promo_revenue = merged_df['REVENUE'].sum()
percent_promo_revenue = (promo_revenue / total_revenue) * 100 if total_revenue != 0 else 0

# Write output to CSV
with open('query_output.csv', 'w', newline='') as csvfile:
    fieldnames = ['TOTAL_REVENUE', 'PROMO_REVENUE', 'PERCENT_PROMO_REVENUE']
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

    writer.writeheader()
    writer.writerow({
        'TOTAL_REVENUE': total_revenue,
        'PROMO_REVENUE': promo_revenue,
        'PERCENT_PROMO_REVENUE': percent_promo_revenue
    })

# Close the MongoDB connection
mongo_client.close()
