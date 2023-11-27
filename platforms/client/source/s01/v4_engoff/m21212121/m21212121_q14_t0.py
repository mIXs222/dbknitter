from pymongo import MongoClient
import pandas as pd
import direct_redis
import datetime

# Connect to MongoDB
client = MongoClient('mongodb', 27017)
mongo_db = client['tpch']
lineitem_collection = mongo_db['lineitem']

# Query MongoDB to get relevant lineitem data
query = {
    "L_SHIPDATE": {
        "$gte": datetime.datetime(1995, 9, 1),
        "$lt": datetime.datetime(1995, 10, 1),
    }
}
projection = {
    "_id": 0,
    "L_PARTKEY": 1,
    "L_EXTENDEDPRICE": 1,
    "L_DISCOUNT": 1
}
lineitem_data = list(lineitem_collection.find(query, projection))
lineitem_df = pd.DataFrame(lineitem_data)

# Connect to Redis
r_client = direct_redis.DirectRedis(host='redis', port=6379, db=0)
part_data = r_client.get('part')
part_df = pd.read_json(part_data)

# Combine the data
merged_df = pd.merge(lineitem_df, part_df, how='inner', left_on='L_PARTKEY', right_on='P_PARTKEY')

# Calculate the revenue
merged_df['REVENUE'] = merged_df['L_EXTENDEDPRICE'] * (1 - merged_df['L_DISCOUNT'])

# Calculate the total and promotional revenue
total_revenue = merged_df['REVENUE'].sum()

# Identify promotion parts, you need to define what constitutes a promotion part
# Assuming P_COMMENT contains the indication for promotional parts, for example with word 'Promo'
merged_df['PROMOTION'] = merged_df['P_COMMENT'].str.contains('Promo')

# Calculate promotional revenue
promo_revenue = merged_df[merged_df['PROMOTION'] == True]['REVENUE'].sum()

# Calculate promotional revenue percentage
promo_percentage = (promo_revenue / total_revenue) * 100 if total_revenue != 0 else 0

# Output the result into CSV
output_df = pd.DataFrame({
    'PROMO_REVENUE': [promo_revenue],
    'TOTAL_REVENUE': [total_revenue],
    'PROMO_PERCENTAGE': [promo_percentage]
})
output_df.to_csv('query_output.csv', index=False)
