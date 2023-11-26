import pymongo
import pandas as pd
from datetime import datetime
from direct_redis import DirectRedis

# Connect to MongoDB
client = pymongo.MongoClient("mongodb", 27017)
mongodb = client['tpch']

# Query MongoDB for 'part' collection
part_df = pd.DataFrame(list(mongodb.part.find(
    {'P_TYPE': {'$regex': '^PROMO'}},
    {'_id': 0, 'P_PARTKEY': 1}
)))

# Connect to Redis
redis = DirectRedis(host='redis', port=6379, db=0)

# Fetch 'lineitem' table from Redis
lineitem_df_str = redis.get('lineitem')
lineitem_df = pd.read_json(lineitem_df_str)
lineitem_df['L_SHIPDATE'] = pd.to_datetime(lineitem_df['L_SHIPDATE'])

# Filter lineitem by date
start_date = datetime(1995, 9, 1)
end_date = datetime(1995, 10, 1)
filtered_lineitem_df = lineitem_df[
    (lineitem_df['L_SHIPDATE'] >= start_date) &
    (lineitem_df['L_SHIPDATE'] < end_date)
]

# Perform the join
result_df = filtered_lineitem_df.merge(part_df, how='inner', left_on='L_PARTKEY', right_on='P_PARTKEY')

# Calculate the promo revenue
result_df['VALUE'] = result_df['L_EXTENDEDPRICE'] * (1 - result_df['L_DISCOUNT'])
promo_revenue = result_df['VALUE'].sum()
total_value = filtered_lineitem_df['L_EXTENDEDPRICE'] * (1 - filtered_lineitem_df['L_DISCOUNT'])
total_revenue = total_value.sum()
promo_revenue_percent = (promo_revenue / total_revenue) * 100 if total_revenue != 0 else None

# Create the final output
output = pd.DataFrame({
    'PROMO_REVENUE': [promo_revenue_percent]
})

# Write to CSV
output.to_csv('query_output.csv', index=False)
