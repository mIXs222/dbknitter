# promotion_effect_query.py
from pymongo import MongoClient
from direct_redis import DirectRedis
import pandas as pd

# Connect to MongoDB
client = MongoClient('mongodb', 27017)
mongo_db = client['tpch']

# Connect to Redis
redis_client = DirectRedis(host='redis', port=6379)

# MongoDB query to fetch promotional parts
parts_collection = mongo_db['part']
promotional_parts_df = pd.DataFrame(list(parts_collection.find(
    {}, {'P_PARTKEY': 1, 'P_RETAILPRICE': 1, '_id': 0}
)))

# Redis query to fetch lineitem data
lineitem_df = redis_client.get('lineitem')
if lineitem_df:
    lineitem_df = pd.read_json(lineitem_df)

# Combine and process data
if lineitem_df is not None and not promotional_parts_df.empty:
    # Filter lineitem by shipdate
    lineitem_df = lineitem_df[(lineitem_df['L_SHIPDATE'] >= '1995-09-01') & (lineitem_df['L_SHIPDATE'] <= '1995-10-01')]

    # Merge to get only promotional parts
    merged_df = pd.merge(lineitem_df, promotional_parts_df, left_on='L_PARTKEY', right_on='P_PARTKEY')

    # Calculate revenue
    merged_df['revenue'] = merged_df['L_EXTENDEDPRICE'] * (1 - merged_df['L_DISCOUNT'])

    # Calculate the total revenue
    total_revenue = merged_df['revenue'].sum()

    # If total_revenue is not 0, calculate percentage, otherwise set percentage to 0
    promotional_revenue_percentage = (total_revenue / merged_df['L_EXTENDEDPRICE'].sum() * 100) if total_revenue else 0

    # Write the result to csv file
    pd.DataFrame({'Promotion Effect Percentage': [promotional_revenue_percentage]}).to_csv('query_output.csv', index=False)
