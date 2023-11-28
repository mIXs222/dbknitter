import pymongo
import pandas as pd
from datetime import datetime
import direct_redis

# Connect to MongoDB
client = pymongo.MongoClient('mongodb', 27017)
mdb = client['tpch']

# Fetch data within the specified timeframe from MongoDB
start_date = datetime(1996, 1, 1)
end_date = datetime(1996, 3, 31)

pipeline = [
    {
        '$match': {
            'L_SHIPDATE': {
                '$gte': start_date,
                '$lte': end_date
            }
        }
    },
    {
        '$group': {
            '_id': '$L_SUPPKEY',
            'TotalRevenue': {
                '$sum': {
                    '$multiply': [
                        '$L_EXTENDEDPRICE', 
                        {'$subtract': [1, '$L_DISCOUNT']}
                    ]
                }
            }
        }
    }
]
lineitems = list(mdb['lineitem'].aggregate(pipeline))

# Convert lineitems to DataFrame
df_lineitems = pd.DataFrame(lineitems)
df_lineitems.rename(columns={'_id': 'S_SUPPKEY', 'TotalRevenue': 'TOTAL_REVENUE'}, inplace=True)

# Connect to Redis
r = direct_redis.DirectRedis(host='redis', port=6379, db=0)
supplier_data = r.get("supplier")

# Convert supplier data to DataFrame
df_supplier = pd.read_json(supplier_data)

# Merge data on supplier key
result = df_supplier.merge(df_lineitems, on='S_SUPPKEY', how='inner')

# Filter out the supplier with the maximum total revenue
max_revenue_supplier = result[result['TOTAL_REVENUE'] == result['TOTAL_REVENUE'].max()]

# Sort the results
max_revenue_supplier.sort_values('S_SUPPKEY', ascending=True, inplace=True)

# Output to CSV
max_revenue_supplier.to_csv('query_output.csv', index=False)
