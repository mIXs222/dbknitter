# Python Code - part_promotion_query.py
import pandas as pd
from pymongo import MongoClient
from datetime import datetime
from direct_redis import DirectRedis

# MongoDB connection
client = MongoClient(host='mongodb', port=27017)
mongodb = client['tpch']
# Tables in MongoDB
partsupp = mongodb['partsupp']
lineitem = mongodb['lineitem']

# Redis connection
redis = DirectRedis(host='redis', port=6379, db=0)

# Function to get data from Redis as DataFrame
def get_redis_df(key):
    df_json = redis.get(key)
    if df_json:
        df = pd.read_json(df_json)
        return df

# Getting data from Redis
nation_df = get_redis_df('nation')
part_df = get_redis_df('part')
supplier_df = get_redis_df('supplier')

# Filter suppliers from CANADA
suppliers_from_canada = supplier_df[supplier_df['S_NATIONKEY'] == nation_df[nation_df['N_NAME'] == 'CANADA']['N_NATIONKEY'].values[0]]

# Get all parts that have 'forest' in their names
forest_parts = part_df[part_df['P_NAME'].str.contains('forest', case=False, na=False)]

# MongoDB query for lineitem - date filtering
start_date = datetime(1994, 1, 1)
end_date = datetime(1995, 1, 1)
lineitem_cursor = lineitem.find({
    'L_SHIPDATE': {'$gte': start_date, '$lt': end_date}
})

# Convert cursor to DataFrame
lineitem_df = pd.DataFrame(list(lineitem_cursor))

# Joining dataframes to filter relevant parts and partsuppliers
lineitem_supp_forest_df = lineitem_df.merge(suppliers_from_canada, how='inner', left_on='L_SUPPKEY', right_on='S_SUPPKEY')
lineitem_supp_forest_df = lineitem_supp_forest_df.merge(forest_parts, how='inner', left_on='L_PARTKEY', right_on='P_PARTKEY')

# Filtering suppliers with excess parts (>50%)
grouped_df = lineitem_supp_forest_df.groupby(['S_SUPPKEY']).agg(total_qty=('L_QUANTITY', 'sum')).reset_index()
excess_suppliers = partsupp.merge(grouped_df[grouped_df['total_qty'] > partsupp['PS_AVAILQTY'] * 0.5], left_on='PS_SUPPKEY', right_on='S_SUPPKEY', how='inner')

# Select relevant columns for output
output_df = excess_suppliers[['S_SUPPKEY', 'S_NAME', 'S_ADDRESS', 'S_PHONE', 'S_ACCTBAL', 'S_COMMENT', 'total_qty']]

# Write to CSV
output_df.to_csv('query_output.csv', index=False)
