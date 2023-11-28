import pymongo
import pandas as pd
from datetime import datetime
import direct_redis

# MongoDB connection and query
mongo_client = pymongo.MongoClient("mongodb", 27017)
mongo_db = mongo_client["tpch"]
lineitem_collection = mongo_db["lineitem"]

# Date range for the query
start_date = datetime(1995, 9, 1)
end_date = datetime(1995, 9, 30)

# Query lineitem collection
lineitem_query = {
    'L_SHIPDATE': {
        '$gte': start_date,
        '$lte': end_date
    }
}
lineitems_df = pd.DataFrame(list(lineitem_collection.find(lineitem_query, projection={'_id': False})))

# Redis connection and query
redis_client = direct_redis.DirectRedis(host='redis', port=6379, db=0)
part_data = redis_client.get('part')
part_df = pd.read_msgpack(part_data)

# Filter parts with PROMO
promo_parts_df = part_df[part_df['P_TYPE'].str.startswith('PROMO')]

# Merge lineitems with promo parts
promo_revenue_df = lineitems_df[lineitems_df['L_PARTKEY'].isin(promo_parts_df['P_PARTKEY'])]
promo_revenue_df['ADJUSTED_PRICE'] = promo_revenue_df['L_EXTENDEDPRICE'] * (1 - promo_revenue_df['L_DISCOUNT'])

# Calculate total adjusted revenue
lineitems_df['ADJUSTED_PRICE'] = lineitems_df['L_EXTENDEDPRICE'] * (1 - lineitems_df['L_DISCOUNT'])
total_revenue = lineitems_df['ADJUSTED_PRICE'].sum()
promo_revenue = promo_revenue_df['ADJUSTED_PRICE'].sum()

# Calculate the promotional revenue percentage
promo_revenue_percentage = (promo_revenue / total_revenue) * 100 if total_revenue else 0

# Save results to CSV
result_df = pd.DataFrame({'PROMO_REVENUE_PERCENTAGE': [promo_revenue_percentage]})
result_df.to_csv('query_output.csv', index=False)
