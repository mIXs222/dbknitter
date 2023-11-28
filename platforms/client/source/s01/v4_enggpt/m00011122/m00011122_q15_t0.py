import pandas as pd
import pymongo
from datetime import datetime
from direct_redis import DirectRedis

# Connect to MongoDB
client = pymongo.MongoClient("mongodb://mongodb:27017/")
db = client["tpch"]
supplier_collection = db["supplier"]

# Query MongoDB for suppliers
suppliers_df = pd.DataFrame(list(supplier_collection.find({}, {
    '_id': 0,
    'S_SUPPKEY': 1,
    'S_NAME': 1,
    'S_ADDRESS': 1,
    'S_PHONE': 1
})))

# Connect to Redis
r = DirectRedis(host='redis', port=6379, db=0)

# Retrieve 'lineitem' as a DataFrame from Redis
lineitem_str = r.get('lineitem')
lineitem_df = pd.read_json(lineitem_str)

# Filter lineitem dataframe for dates between January 1, 1996, and March 31, 1996
start_date = datetime(1996, 1, 1)
end_date = datetime(1996, 3, 31)
lineitem_df['L_SHIPDATE'] = pd.to_datetime(lineitem_df['L_SHIPDATE'])
filtered_lineitem_df = lineitem_df[(lineitem_df['L_SHIPDATE'] >= start_date) & (lineitem_df['L_SHIPDATE'] <= end_date)]

# Calculate total revenue per supplier in the given date range
filtered_lineitem_df['TOTAL_REVENUE'] = filtered_lineitem_df['L_EXTENDEDPRICE'] * (1 - filtered_lineitem_df['L_DISCOUNT'])
revenue_per_supplier = filtered_lineitem_df.groupby('L_SUPPKEY')['TOTAL_REVENUE'].sum().reset_index()

# Merge supplier data with revenue data
merged_df = pd.merge(suppliers_df, revenue_per_supplier, left_on='S_SUPPKEY', right_on='L_SUPPKEY')

# Find the supplier with the highest total revenue
max_revenue_supplier = merged_df.loc[merged_df['TOTAL_REVENUE'].idxmax()]

# Write the result to query_output.csv
max_revenue_supplier.to_csv('query_output.csv', index=False)
