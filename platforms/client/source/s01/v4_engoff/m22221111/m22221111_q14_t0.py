from pymongo import MongoClient
import direct_redis
import pandas as pd
from datetime import datetime

# Connect to MongoDB
client = MongoClient('mongodb', 27017)
db = client['tpch']
lineitem_collection = db['lineitem']

# Query for MongoDB
start_date = datetime(1995, 9, 1)
end_date = datetime(1995, 10, 1)
mongo_query = {
    'L_SHIPDATE': {'$gte': start_date, '$lt': end_date}
}
projection = {
    'L_PARTKEY': 1,
    'L_EXTENDEDPRICE': 1,
    'L_DISCOUNT': 1,
    '_id': 0
}
lineitem_data = list(lineitem_collection.find(mongo_query, projection))
lineitem_df = pd.DataFrame(lineitem_data)

# Connect to Redis
r = direct_redis.DirectRedis(host='redis', port=6379, db=0)

# Get part data from Redis
part_keys = r.keys('part:*')
part_data = []
for key in part_keys:
    part_record = r.get(key)
    if part_record:
        part_data.append(part_record)
part_df = pd.DataFrame(part_data)

# Cast columns to appropriate data types
part_df = part_df.astype({
    'P_PARTKEY': 'int64',
    'P_RETAILPRICE': 'float'
})
lineitem_df = lineitem_df.astype({
    'L_PARTKEY': 'int64',
    'L_EXTENDEDPRICE': 'float',
    'L_DISCOUNT': 'float'
})

# Filter promotional parts and calculate revenue
promotional_parts = part_df.query("P_NAME.str.contains('Promo')", engine='python')

lineitem_df['Revenue'] = lineitem_df['L_EXTENDEDPRICE'] * (1 - lineitem_df['L_DISCOUNT'])
merged_df = lineitem_df.merge(promotional_parts, how='inner', left_on='L_PARTKEY', right_on='P_PARTKEY')

# Calculate total revenue and promotional revenue
total_revenue = lineitem_df['Revenue'].sum()
promotional_revenue = merged_df['Revenue'].sum()

# Calculate promotional revenue percentage
promotion_revenue_percentage = (promotional_revenue / total_revenue) * 100

# Save output to query_output.csv
output_df = pd.DataFrame({
    "Promotion Revenue Percentage": [promotion_revenue_percentage]
})
output_df.to_csv('query_output.csv', index=False)
