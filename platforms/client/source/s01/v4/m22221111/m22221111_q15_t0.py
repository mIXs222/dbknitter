from pymongo import MongoClient
import pandas as pd
from datetime import datetime, timedelta
from direct_redis import DirectRedis

# MongoDB connection
client = MongoClient('mongodb', port=27017)
db = client['tpch']
lineitem_collection = db.lineitem

# Redis connection
redis_client = DirectRedis(host='redis', port=6379, db=0)

# Retrieve lineitem data
lineitem_query = {
    'L_SHIPDATE': {
        '$gte': datetime(1996, 1, 1),
        '$lt': datetime(1996, 1, 1) + timedelta(days=90)
    }
}
lineitem_projection = {
    'L_SUPPKEY': 1, 'L_EXTENDEDPRICE': 1, 'L_DISCOUNT': 1
}
lineitem_cursor = lineitem_collection.find(lineitem_query, lineitem_projection)

lineitem_df = pd.DataFrame(list(lineitem_cursor))
lineitem_df['TOTAL_REVENUE'] = lineitem_df['L_EXTENDEDPRICE'] * (1 - lineitem_df['L_DISCOUNT'])
revenue0_df = lineitem_df.groupby('L_SUPPKEY')['TOTAL_REVENUE'].sum().reset_index().rename(columns={'L_SUPPKEY': 'SUPPLIER_NO'})

# Retrieve supplier data
supplier_df_data = redis_client.get('supplier')
supplier_df = pd.read_msgpack(supplier_df_data)
supplier_df['S_SUPPKEY'] = supplier_df['S_SUPPKEY'].astype(int)

# Merge and process data
merged_df = supplier_df.merge(revenue0_df, how='inner', left_on='S_SUPPKEY', right_on='SUPPLIER_NO')
max_revenue = merged_df['TOTAL_REVENUE'].max()
result_df = merged_df[merged_df['TOTAL_REVENUE'] == max_revenue].sort_values(by='S_SUPPKEY')

# Writing to CSV
result_df.to_csv('query_output.csv', index=False)
