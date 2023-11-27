# top_supplier_query.py

from pymongo import MongoClient
import direct_redis
import pandas as pd
import datetime

# Connect to MongoDB
mongo_client = MongoClient(host='mongodb', port=27017)
mongo_db = mongo_client['tpch']
mongo_supplier = mongo_db['supplier']

# Connect to Redis
r = direct_redis.DirectRedis(host='redis', port=6379, db=0)

# Get suppliers from MongoDB
df_suppliers = pd.DataFrame(list(mongo_supplier.find({}, {
    '_id': 0,
    'S_SUPPKEY': 1,
    'S_NAME': 1,
    'S_ADDRESS': 1,
    'S_NATIONKEY': 1,
    'S_PHONE': 1,
    'S_ACCTBAL': 1,
    'S_COMMENT': 1
})))

# Convert Redis data to DataFrame
df_lineitem = pd.read_msgpack(r.get('lineitem'))

# Filter lineitem for the given date range
start_date = datetime.datetime(1996, 1, 1)
end_date = datetime.datetime(1996, 4, 1)
mask = (df_lineitem['L_SHIPDATE'] >= start_date) & (df_lineitem['L_SHIPDATE'] < end_date)
df_lineitem_filtered = df_lineitem.loc[mask]

# Calculate total revenue by supplier
df_lineitem_filtered['TOTAL_REVENUE'] = df_lineitem_filtered['L_EXTENDEDPRICE'] * (1 - df_lineitem_filtered['L_DISCOUNT'])
df_total_revenue = df_lineitem_filtered.groupby('L_SUPPKEY')['TOTAL_REVENUE'].sum().reset_index()

# Identify the top supplier(s)
max_revenue = df_total_revenue['TOTAL_REVENUE'].max()
df_top_supplier_revenue = df_total_revenue[df_total_revenue['TOTAL_REVENUE'] == max_revenue]

# Merge to get all supplier details
df_top_suppliers = pd.merge(df_top_supplier_revenue, df_suppliers, left_on='L_SUPPKEY', right_on='S_SUPPKEY', how='left')

# Select required columns and sort by supplier number
df_top_suppliers = df_top_suppliers[['S_SUPPKEY', 'S_NAME', 'S_ADDRESS', 'S_NATIONKEY', 'S_PHONE', 'S_ACCTBAL', 'TOTAL_REVENUE', 'S_COMMENT']]
df_top_suppliers.sort_values('S_SUPPKEY', inplace=True)

# Write results to CSV
df_top_suppliers.to_csv('query_output.csv', index=False)
