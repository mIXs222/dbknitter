# python_code.py
import pymongo
import pandas as pd
from datetime import datetime
from direct_redis import DirectRedis

# Function to connect to MongoDB
def connect_to_mongo(host, port, db_name):
    client = pymongo.MongoClient(host, port)
    db = client[db_name]
    return db

# Function to read data from Redis
def read_from_redis(host, port, db_name, table_name):
    r = DirectRedis(host=host, port=port, db=db_name)
    data = r.get(table_name)
    df = pd.read_json(data)
    return df

# Connect to MongoDB
mongo_db = connect_to_mongo(host='mongodb', port=27017, db_name='tpch')
# Retrieve 'supplier' collection as DataFrame
supplier_df = pd.DataFrame(list(mongo_db.supplier.find()))

# Connect to Redis and load 'lineitem' as DataFrame
lineitem_df = read_from_redis(host='redis', port=6379, db_name=0, table_name='lineitem')

# Convert string dates to datetime objects
lineitem_df['L_SHIPDATE'] = pd.to_datetime(lineitem_df['L_SHIPDATE'])

# Filter line items within the specified date range
start_date = datetime(1996, 1, 1)
end_date = datetime(1996, 4, 1)
filtered_lineitem_df = lineitem_df[(lineitem_df['L_SHIPDATE'] >= start_date) & (lineitem_df['L_SHIPDATE'] < end_date)]

# Calculate revenue contribution of suppliers
filtered_lineitem_df['REVENUE'] = filtered_lineitem_df['L_EXTENDEDPRICE'] * (1 - filtered_lineitem_df['L_DISCOUNT'])
revenue_per_supplier = filtered_lineitem_df.groupby('L_SUPPKEY')['REVENUE'].sum().reset_index()

# Find the maximum revenue
max_revenue = revenue_per_supplier['REVENUE'].max()

# Find top suppliers by revenue
top_suppliers = revenue_per_supplier[revenue_per_supplier['REVENUE'] == max_revenue]

# Sort top suppliers by supplier number
top_suppliers_sorted = top_suppliers.sort_values(by='L_SUPPKEY')

# Select only the 'S_SUPPKEY' and 'REVENUE' columns
top_suppliers_final = supplier_df[supplier_df['S_SUPPKEY'].isin(top_suppliers_sorted['L_SUPPKEY'])]
top_suppliers_final['REVENUE'] = top_suppliers_sorted['REVENUE'].values

# Save the result to a CSV file
top_suppliers_final[['S_SUPPKEY', 'S_NAME', 'REVENUE']].to_csv('query_output.csv', index=False)
