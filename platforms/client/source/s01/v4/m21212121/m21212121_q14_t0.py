import pymongo
import pandas as pd
import direct_redis
import datetime

# MongoDB connection and query
client = pymongo.MongoClient("mongodb", 27017)
mongodb = client.tpch
lineitem_df = pd.DataFrame(list(mongodb.lineitem.find(
    {"L_SHIPDATE": {"$gte": datetime.datetime(1995, 9, 1), "$lt": datetime.datetime(1995, 10, 1)}},
    {"L_PARTKEY": 1, "L_EXTENDEDPRICE": 1, "L_DISCOUNT": 1}
)))

# Redis connection and query
redis_conn = direct_redis.DirectRedis(host="redis", port=6379, db=0)
part_df = pd.DataFrame(eval(redis_conn.get('part')))  # Dangerous: eval should be replaced with a safe parser in production

# Merge the dataframes
merged_df = pd.merge(lineitem_df, part_df, how='inner', left_on='L_PARTKEY', right_on='P_PARTKEY')

# Calculate PROMO_REVENUE
promo_revenue = merged_df[merged_df['P_TYPE'].str.startswith('PROMO')]['L_EXTENDEDPRICE'] * (1 - merged_df['L_DISCOUNT'])
total_revenue = merged_df['L_EXTENDEDPRICE'] * (1 - merged_df['L_DISCOUNT'])

# Calculate the ratio
result = (100.00 * promo_revenue.sum()) / total_revenue.sum() if total_revenue.sum() else None

# Write to CSV
pd.DataFrame({'PROMO_REVENUE': [result]}).to_csv('query_output.csv', index=False)
