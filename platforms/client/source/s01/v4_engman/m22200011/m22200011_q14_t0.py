# promotion_effect_query.py
from pymongo import MongoClient
import pandas as pd
from datetime import datetime
from direct_redis import DirectRedis

# MongoDB connection and data retrieval
mongo_client = MongoClient('mongodb', 27017)
mongo_db = mongo_client['tpch']
mongo_collection = mongo_db['lineitem']

# Filter by date range in MongoDB query
start_date = datetime(1995, 9, 1)
end_date = datetime(1995, 10, 1)
lineitems = list(mongo_collection.find({
    "L_SHIPDATE": {"$gte": start_date, "$lt": end_date}
}, {'_id': False}))

# Convert to DataFrame
lineitem_df = pd.DataFrame(lineitems)

# Calculating revenue
lineitem_df['revenue'] = lineitem_df['L_EXTENDEDPRICE'] * (1 - lineitem_df['L_DISCOUNT'])

# Redis connection and data retrieval
redis_client = DirectRedis(host='redis', port=6379, db=0)
part_df = pd.read_json(redis_client.get('part'), orient='records')

# Assuming part promotion details are flagged in P_COMMENT with the string 'Promo'
is_promo_part = part_df['P_COMMENT'].str.contains('Promo')
promo_parts = part_df[is_promo_part]

# Combine the results
promo_parts_set = set(promo_parts['P_PARTKEY'])
lineitem_df['is_promo'] = lineitem_df['L_PARTKEY'].isin(promo_parts_set)
promo_revenue = lineitem_df[lineitem_df['is_promo']]['revenue'].sum()
total_revenue = lineitem_df['revenue'].sum()

# Calculate and save the promotion effect
promo_effect = (promo_revenue / total_revenue) if total_revenue else 0
promo_effect_percentage = promo_effect * 100

# Saving to CSV
output = pd.DataFrame([{'Promotion Effect (%)': promo_effect_percentage}])
output.to_csv('query_output.csv', index=False)
