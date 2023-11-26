import pymongo
import pandas as pd
from datetime import datetime
from direct_redis import DirectRedis

# Connect to MongoDB
client = pymongo.MongoClient('mongodb', 27017)
mongodb = client['tpch']

# Get lineitem table from MongoDB
lineitem_columns = [
    'L_ORDERKEY', 'L_PARTKEY', 'L_SUPPKEY', 'L_LINENUMBER', 'L_QUANTITY',
    'L_EXTENDEDPRICE', 'L_DISCOUNT', 'L_TAX', 'L_RETURNFLAG', 'L_LINESTATUS',
    'L_SHIPDATE', 'L_COMMITDATE', 'L_RECEIPTDATE', 'L_SHIPINSTRUCT', 'L_SHIPMODE',
    'L_COMMENT'
]
lineitem = pd.DataFrame(list(mongodb['lineitem'].find(
    {'L_SHIPDATE': {
        '$gte': datetime(1996, 1, 1), 
        '$lt': datetime(1996, 4, 1)   
    }},
    projection=lineitem_columns)))

# Calculate the TOTAL_REVENUE
lineitem['TOTAL_REVENUE'] = lineitem['L_EXTENDEDPRICE'] * (1 - lineitem['L_DISCOUNT'])
revenue0 = lineitem.groupby('L_SUPPKEY').agg(TOTAL_REVENUE=('TOTAL_REVENUE', 'sum')).reset_index()
max_revenue = revenue0['TOTAL_REVENUE'].max()
revenue0 = revenue0[revenue0['TOTAL_REVENUE'] == max_revenue]

# Connect to Redis
redis_client = DirectRedis(host='redis', port=6379, db=0)

# Get supplier table from Redis
supplier_df = pd.read_json(redis_client.get('supplier'), orient='records')

# Combine results
result = pd.merge(supplier_df, revenue0, left_on='S_SUPPKEY', right_on='L_SUPPKEY')
result = result[['S_SUPPKEY', 'S_NAME', 'S_ADDRESS', 'S_PHONE', 'TOTAL_REVENUE']]
result.sort_values('S_SUPPKEY', inplace=True)

# Write output to CSV
result.to_csv('query_output.csv', index=False)
