# promotion_effect_query.py

from pymongo import MongoClient
from direct_redis import DirectRedis
import pandas as pd

# MongoDB connection
mongo_client = MongoClient('mongodb', 27017)
mongo_db = mongo_client['tpch']
mongo_lineitem = mongo_db['lineitem']

# Redis connection
redis_client = DirectRedis(host='redis', port=6379, db=0)

# Mongo query to select the documents from lineitem within the dates
lineitems = mongo_lineitem.find({
    'L_SHIPDATE': {'$gte': '1995-09-01', '$lt': '1995-10-01'}
})

# Create DataFrame from mongo documents
lineitem_df = pd.DataFrame(list(lineitems))

# Redis query to get part table
part_raw = redis_client.get('part')
part_df = pd.read_json(part_raw)

# Join lineitem and part tables on L_PARTKEY == P_PARTKEY
merged_df = lineitem_df.merge(part_df, how='inner', left_on='L_PARTKEY', right_on='P_PARTKEY')

# Calculate revenue and check promotional status for parts
merged_df['REVENUE'] = merged_df['L_EXTENDEDPRICE'] * (1 - merged_df['L_DISCOUNT'])
promotional_revenue = merged_df['REVENUE'].sum()

# Calculate the total revenue so we can calculate the percentage
total_revenue = lineitem_df['L_EXTENDEDPRICE'].sum() * (1 - lineitem_df['L_DISCOUNT']).sum()

# Calculate final promotional percentage
promotional_percentage = (promotional_revenue / total_revenue) * 100

# Save results to a CSV
result_df = pd.DataFrame({
    'Promotional Revenue Percentage': [promotional_percentage]
})
result_df.to_csv('query_output.csv', index=False)
