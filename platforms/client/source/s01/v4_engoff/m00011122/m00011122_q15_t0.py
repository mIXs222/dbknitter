import pymongo
import pandas as pd
import direct_redis
import datetime

# Connect to MongoDB
mongo_client = pymongo.MongoClient("mongodb", 27017)
mongo_db = mongo_client["tpch"]
mongo_supplier_collection = mongo_db["supplier"]

# Connect to Redis
redis_client = direct_redis.DirectRedis(host="redis", port=6379, db=0)

# Convert Redis data to DataFrame
lineitem_data = redis_client.get('lineitem')
lineitem_df = pd.read_json(lineitem_data)

# Filter lineitem DataFrame for the date range
start_date = datetime.datetime(1996, 1, 1)
end_date = datetime.datetime(1996, 4, 1)
lineitem_df['L_SHIPDATE'] = pd.to_datetime(lineitem_df['L_SHIPDATE'])
filtered_lineitem_df = lineitem_df[(lineitem_df['L_SHIPDATE'] >= start_date) & (lineitem_df['L_SHIPDATE'] <= end_date)]

# Calculate revenue contribution for each supplier
filtered_lineitem_df['REVENUE'] = filtered_lineitem_df['L_EXTENDEDPRICE'] * (1 - filtered_lineitem_df['L_DISCOUNT'])
supplier_revenue = filtered_lineitem_df.groupby('L_SUPPKEY')['REVENUE'].sum().reset_index()

# Fetch supplier data from MongoDB and convert to DataFrame
mongo_suppliers = list(mongo_supplier_collection.find({}, {'_id': 0}))
suppliers_df = pd.DataFrame(mongo_suppliers)

# Merge supplier data with revenue data
merged_data = pd.merge(suppliers_df, supplier_revenue, left_on='S_SUPPKEY', right_on='L_SUPPKEY')

# Find the top supplier(s)
max_revenue = merged_data['REVENUE'].max()
top_suppliers = merged_data[merged_data['REVENUE'] == max_revenue].sort_values(by='S_SUPPKEY')

# Output to CSV
top_suppliers.to_csv('query_output.csv', index=False)
