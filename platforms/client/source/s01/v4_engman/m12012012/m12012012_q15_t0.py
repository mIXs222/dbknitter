from pymongo import MongoClient
import pandas as pd
import direct_redis

# Connection details
mongodb_host = 'mongodb'
mongodb_port = 27017
mongodb_db_name = 'tpch'

redis_host = 'redis'
redis_port = 6379
redis_db_name = '0'

# Connect to MongoDB
mongo_client = MongoClient(host=mongodb_host, port=mongodb_port)
mongodb = mongo_client[mongodb_db_name]
supplier_collection = mongodb['supplier']

# Fetch supplier data from MongoDB
supplier_data = list(supplier_collection.find(
    {},
    {'_id': 0, 'S_SUPPKEY': 1, 'S_NAME': 1, 'S_ADDRESS': 1, 'S_PHONE': 1}
))
supplier_df = pd.DataFrame(supplier_data)

# Connect to Redis
r = direct_redis.DirectRedis(host=redis_host, port=redis_port)

# Get lineitem data from Redis (assuming data has been converted to a pandas dataframe)
lineitem_data = r.get('lineitem_data')
lineitem_df = pd.read_json(lineitem_data)

# Filter lineitem data by date
start_date = pd.to_datetime('1996-01-01')
end_date = pd.to_datetime('1996-04-01')
lineitem_df['L_SHIPDATE'] = pd.to_datetime(lineitem_df['L_SHIPDATE'])
filtered_lineitem = lineitem_df[
    (lineitem_df['L_SHIPDATE'] >= start_date) & 
    (lineitem_df['L_SHIPDATE'] <= end_date)
]

# Calculate the revenue
filtered_lineitem['TOTAL_REVENUE'] = (
    filtered_lineitem['L_EXTENDEDPRICE'] * 
    (1 - filtered_lineitem['L_DISCOUNT'])
)

# Group by supplier and calculate total revenue
grouped_revenue = filtered_lineitem.groupby('L_SUPPKEY')['TOTAL_REVENUE'].sum().reset_index()
grouped_revenue.columns = ['S_SUPPKEY', 'TOTAL_REVENUE']

# Merge supplier information with the revenue
result = supplier_df.merge(grouped_revenue, on='S_SUPPKEY')
result_sorted = result.sort_values(by=['TOTAL_REVENUE', 'S_SUPPKEY'], ascending=[False, True])

# Get the suppliers with the top revenue (assuming there could be ties)
top_revenue = result_sorted['TOTAL_REVENUE'].max()
top_suppliers = result_sorted[result_sorted['TOTAL_REVENUE'] == top_revenue]

# Write to CSV
top_suppliers.to_csv('query_output.csv', index=False)
