from pymongo import MongoClient
from datetime import datetime
import pandas as pd
import direct_redis

# MongoDB connection setup
mongo_client = MongoClient('mongodb', 27017)
mongo_db = mongo_client['tpch']
part_collection = mongo_db['part']

# Fetch promotional parts from MongoDB 'part' collection
mongo_query = {
    "$or": [
        {"P_NAME": {"$regex": ".*Promo.*", "$options": "i"}},
        {"P_COMMENT": {"$regex": ".*Promo.*", "$options": "i"}}
    ]
}
promotional_parts = list(part_collection.find(mongo_query, {"_id": 0, "P_PARTKEY": 1}))
promotional_part_keys = [item['P_PARTKEY'] for item in promotional_parts]

# Redis connection setup
r = direct_redis.DirectRedis(host='redis', port=6379, db=0)

# Fetch 'lineitem' data from Redis and load it into a DataFrame
lineitem_str = r.get('lineitem')
lineitem_df = pd.read_json(lineitem_str)

# Filter lineitem data for the date range and promotional parts
date_start = datetime(1995, 9, 1)
date_end = datetime(1995, 10, 1)
filtered_lineitem_df = lineitem_df[
    (lineitem_df['L_SHIPDATE'] >= date_start) &
    (lineitem_df['L_SHIPDATE'] < date_end) &
    (lineitem_df['L_PARTKEY'].isin(promotional_part_keys))
]

# Calculate the revenue
filtered_lineitem_df['REVENUE'] = (
    filtered_lineitem_df['L_EXTENDEDPRICE'] * (1 - filtered_lineitem_df['L_DISCOUNT'])
)

# Calculate the total revenue and the promotional revenue
total_promo_revenue = filtered_lineitem_df['REVENUE'].sum()
total_revenue = lineitem_df['L_EXTENDEDPRICE'] * (1 - lineitem_df['L_DISCOUNT'])
total_revenue_sum = total_revenue.sum()

# Calculate the percentage of promotional revenue
promo_revenue_percentage = (total_promo_revenue / total_revenue_sum) * 100 if total_revenue_sum else 0

# Write the result to a CSV file
output = pd.DataFrame({
    'Total Revenue': [total_revenue_sum],
    'Promotional Revenue': [total_promo_revenue],
    'Percentage': [promo_revenue_percentage]
})
output.to_csv('query_output.csv', index=False)
