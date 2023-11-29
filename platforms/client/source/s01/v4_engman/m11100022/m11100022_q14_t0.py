# query_code.py
import pandas as pd
from pymongo import MongoClient
from direct_redis import DirectRedis

# MongoDB connection
client = MongoClient(host='mongodb', port=27017)
mongodb = client['tpch']
part_collection = mongodb['part']

# Query to retrieve parts that are considered promotional
promotional_parts_df = pd.DataFrame(list(part_collection.find({})))

# Redis connection
redis_client = DirectRedis(host='redis', port=6379, db=0)
lineitem_df = redis_client.get('lineitem')

if lineitem_df is not None:
    # Convert the Redis response to DataFrame and filter the dates
    lineitem_df = pd.DataFrame(eval(lineitem_df), index=[0])
    lineitem_df = lineitem_df[(lineitem_df['L_SHIPDATE'] >= '1995-09-01') & (lineitem_df['L_SHIPDATE'] <= '1995-10-01')]
    
    # Inner join with promotional parts on part key
    combined_df = pd.merge(lineitem_df, promotional_parts_df, how='inner', left_on='L_PARTKEY', right_on='P_PARTKEY')
    
    # Calculating revenue
    combined_df['REVENUE'] = combined_df['L_EXTENDEDPRICE'] * (1 - combined_df['L_DISCOUNT'])

    # Calculate the total revenue from promotional parts
    total_revenue = combined_df['REVENUE'].sum()

    # Calculate the total revenue across all parts
    total_market_revenue = lineitem_df['L_EXTENDEDPRICE'] * (1 - lineitem_df['L_DISCOUNT'])
    total_market_revenue = total_market_revenue.sum()

    # Calculate the percentage of revenue from promotional parts
    promo_revenue_percentage = (total_revenue / total_market_revenue) * 100 if total_market_revenue else 0

    # Create a result DataFrame
    result_df = pd.DataFrame({
        "StartDate": "1995-09-01",
        "EndDate": "1995-10-01",
        "PromoRevenue": [total_revenue],
        "TotalMarketRevenue": [total_market_revenue],
        "Percentage": [promo_revenue_percentage]
    })

    # Write results to CSV
    result_df.to_csv('query_output.csv', index=False)
else:
    print('No data found for the specified table in Redis.')
