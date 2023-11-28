import pandas as pd
from pymongo import MongoClient
from direct_redis import DirectRedis

# Connect to MongoDB
client = MongoClient('mongodb', 27017)
mongodb = client['tpch']

# Load lineitem table
lineitem_df = pd.DataFrame(list(mongodb.lineitem.find(
    {
        'L_LINESTATUS': 'F',
        'L_RECEIPTDATE': {'$gt': '$L_COMMITDATE'}
    },
    {
        '_id': 0,
        'L_ORDERKEY': 1,
        'L_SUPPKEY': 1,
        'L_RECEIPTDATE': 1,
        'L_COMMITDATE': 1
    }
)))

# Connect to Redis
redis = DirectRedis(host='redis', port=6379, db=0)

# Load nation, supplier, and orders tables
nation_df = pd.read_json(redis.get('nation'))
supplier_df = pd.read_json(redis.get('supplier'))
orders_df = pd.read_json(redis.get('orders'))

# Filter suppliers located in Saudi Arabia
saudi_suppliers_df = supplier_df.loc[supplier_df['S_NATIONKEY'].isin(nation_df.loc[nation_df['N_NAME'] == 'SAUDI ARABIA', 'N_NATIONKEY'])]

# Filter orders with an order status of 'F'
fulfilled_orders_df = orders_df[orders_df['O_ORDERSTATUS'] == 'F']

# Merge dataframes
merged_df = lineitem_df.merge(saudi_suppliers_df, left_on='L_SUPPKEY', right_on='S_SUPPKEY')
merged_df = merged_df.merge(fulfilled_orders_df, left_on='L_ORDERKEY', right_on='O_ORDERKEY')

# Define subquery filter functions
def exists_other_supplier_lineitems(row):
    return lineitem_df[(lineitem_df['L_ORDERKEY'] == row['L_ORDERKEY']) & (lineitem_df['L_SUPPKEY'] != row['L_SUPPKEY'])].any()

def not_exists_later_receipt_lineitems(row):
    return not lineitem_df[(lineitem_df['L_ORDERKEY'] == row['L_ORDERKEY']) & (lineitem_df['L_SUPPKEY'] != row['L_SUPPKEY']) & (lineitem_df['L_RECEIPTDATE'] > row['L_COMMITDATE'])].any()

# Apply subquery filters
filtered_df = merged_df[merged_df.apply(exists_other_supplier_lineitems, axis=1) & merged_df.apply(not_exists_later_receipt_lineitems, axis=1)]

# Group by supplier and count waiting times
result_df = filtered_df.groupby('S_NAME').agg(NUMWAIT=('L_ORDERKEY', 'count')).reset_index()

# Sort the results
sorted_result_df = result_df.sort_values(by=['NUMWAIT', 'S_NAME'], ascending=[False, True])

# Write the results to a CSV file
sorted_result_df.to_csv('query_output.csv', index=False)
