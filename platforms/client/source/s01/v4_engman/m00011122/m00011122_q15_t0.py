# top_supplier_query.py
import pymongo
from bson.son import SON
from datetime import datetime
import direct_redis
import pandas as pd

# MongoDB connection
mongo_client = pymongo.MongoClient('mongodb', 27017)
mongo_db = mongo_client['tpch']
supplier_collection = mongo_db['supplier']

# Redis connection
redis = direct_redis.DirectRedis(host='redis', port=6379, db=0)

# Constants for date range
start_date = datetime(1996, 1, 1)
end_date = datetime(1996, 4, 1)

# Execute query
lineitem_df = redis.get('lineitem')
# Assuming lineitem data is stored in a specific format, convert to DataFrame
lineitem_df = pd.read_json(lineitem_df)

# Filter lineitem by date range
lineitem_df['L_SHIPDATE'] = pd.to_datetime(lineitem_df['L_SHIPDATE'])
filtered_lineitems = lineitem_df[
    (lineitem_df['L_SHIPDATE'] >= start_date) & (lineitem_df['L_SHIPDATE'] <= end_date)
]

# Calculate revenue for each supplier
filtered_lineitems['TOTAL_REVENUE'] = filtered_lineitems['L_EXTENDEDPRICE'] * (1 - filtered_lineitems['L_DISCOUNT'])
revenue_per_supplier = filtered_lineitems.groupby('L_SUPPKEY')['TOTAL_REVENUE'].sum().reset_index()

# Find the maximum revenue
max_revenue = revenue_per_supplier['TOTAL_REVENUE'].max()

# Suppliers with max revenue
top_suppliers_keys = revenue_per_supplier[revenue_per_supplier['TOTAL_REVENUE'] == max_revenue]['L_SUPPKEY']

# Get the required supplier information from MongoDB
pipeline = [
    {
        '$match': {
            'S_SUPPKEY': {
                '$in': top_suppliers_keys.tolist()
            }
        }
    },
    {
        '$project': {
            '_id': 0,
            'S_SUPPKEY': 1,
            'S_NAME': 1,
            'S_ADDRESS': 1,
            'S_PHONE': 1,
            'TOTAL_REVENUE': 1,
        }
    },
    {
        '$sort': SON([('S_SUPPKEY', 1)])
    }
]

top_suppliers_info = list(supplier_collection.aggregate(pipeline))

# Convert to DataFrame to write to CSV
top_suppliers_df = pd.DataFrame(top_suppliers_info)
top_suppliers_df['TOTAL_REVENUE'] = top_suppliers_df['S_SUPPKEY'].map(
    revenue_per_supplier.set_index('L_SUPPKEY')['TOTAL_REVENUE']
)

# Write the result to CSV
top_suppliers_df.to_csv('query_output.csv', index=False)
