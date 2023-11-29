# query_code.py

from pymongo import MongoClient
from datetime import datetime
import pandas as pd

# MongoDB Connection
mongo_client = MongoClient('mongodb', 27017)
mongo_db = mongo_client['tpch']

# Get lineitem data from MongoDB
lineitem_pipeline = [
    {
        '$match': {
            'L_SHIPDATE': {
                '$gte': datetime(1996, 1, 1),
                '$lt': datetime(1996, 4, 1)
            }
        }
    },
    {
        '$group': {
            '_id': '$L_SUPPKEY',
            'TOTAL_REVENUE': {
                '$sum': {
                    '$multiply': [
                        '$L_EXTENDEDPRICE',
                        {'$subtract': [1, '$L_DISCOUNT']}
                    ]
                }
            }
        }
    },
    {'$sort': {'TOTAL_REVENUE': -1, '_id': 1}}
]

lineitem_data = list(mongo_db.lineitem.aggregate(lineitem_pipeline))
max_revenue = lineitem_data[0]['TOTAL_REVENUE'] if lineitem_data else None
top_suppliers = [d['_id'] for d in lineitem_data if d['TOTAL_REVENUE'] == max_revenue]

# Redis Connection and Query
import direct_redis

redis_client = direct_redis.DirectRedis(host='redis', port=6379, db=0)
supplier_data = redis_client.get('supplier')

# Convert to Pandas DataFrame
suppliers_df = pd.read_json(supplier_data)

# Filtering the top suppliers
top_suppliers_df = suppliers_df[suppliers_df['S_SUPPKEY'].isin(top_suppliers)]

# Merge lineitem and supplier data
top_suppliers_revenue_df = pd.merge(
    top_suppliers_df,
    pd.DataFrame(lineitem_data),
    left_on='S_SUPPKEY',
    right_on='_id'
)

top_suppliers_revenue_df = top_suppliers_revenue_df.rename(columns={"TOTAL_REVENUE": "TOTAL_REVENUE"}).sort_values(by="S_SUPPKEY")

# Output relevant fields
top_suppliers_revenue_df = top_suppliers_revenue_df[['S_SUPPKEY', 'S_NAME', 'S_ADDRESS', 'S_PHONE', 'TOTAL_REVENUE']]

# Save to CSV
top_suppliers_revenue_df.to_csv('query_output.csv', index=False)
