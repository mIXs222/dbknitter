# Python code: execute_query.py

import pandas as pd
from pymongo import MongoClient
import direct_redis

# MongoDB connection
client = MongoClient('mongodb', 27017)
db = client['tpch']
# Read collections into Pandas DataFrames
nation_df = pd.DataFrame(list(db.nation.find({}, {'_id': 0})))
orders_df = pd.DataFrame(list(db.orders.find({'O_ORDERSTATUS': 'F'}, {'_id': 0})))

# Redis connection
r = direct_redis.DirectRedis(host='redis', port=6379, db=0)
# Read data into Pandas DataFrames
supplier_df = pd.read_json(r.get('supplier'))
lineitem_df = pd.read_json(r.get('lineitem'))

# Perform analysis
# Filter suppliers located in Saudi Arabia
saudi_nations = nation_df[nation_df['N_NAME'] == 'SAUDI ARABIA']
saudi_suppliers = supplier_df[supplier_df['S_NATIONKEY'].isin(saudi_nations['N_NATIONKEY'])]

# Merge suppliers with lineitems
supplier_lineitem = pd.merge(saudi_suppliers, lineitem_df, left_on='S_SUPPKEY', right_on='L_SUPPKEY', how='inner')

# Merge the result with orders
orders_lineitem = pd.merge(supplier_lineitem, orders_df, left_on='L_ORDERKEY', right_on='O_ORDERKEY', how='inner')

# Additional filtering based on lineitem details
filtered_lineitem = orders_lineitem[(orders_lineitem['L_RECEIPTDATE'] > orders_lineitem['L_COMMITDATE'])]

# Conditions for EXISTS subqueries
def exists_different_supplier(row, dataframe):
    return dataframe[(dataframe['L_ORDERKEY'] == row['L_ORDERKEY']) & (dataframe['L_SUPPKEY'] != row['L_SUPPKEY'])].empty

def not_exists_later_receipt(row, dataframe):
    return not dataframe[(dataframe['L_ORDERKEY'] == row['L_ORDERKEY']) & (dataframe['L_SUPPKEY'] != row['L_SUPPKEY']) & (dataframe['L_RECEIPTDATE'] > row['L_COMMITDATE'])].empty

filtered_lineitem['EXISTS_DIFF_SUPPLIER'] = filtered_lineitem.apply(exists_different_supplier, axis=1, dataframe=filtered_lineitem)
filtered_lineitem['NOT_EXISTS_LATER_RECEIPT'] = filtered_lineitem.apply(not_exists_later_receipt, axis=1, dataframe=filtered_lineitem)

# Apply subquery filters
final_result = filtered_lineitem[filtered_lineitem['EXISTS_DIFF_SUPPLIER'] & filtered_lineitem['NOT_EXISTS_LATER_RECEIPT']]

# Group by supplier and calculate waiting times
final_grouped = final_result.groupby('S_NAME').size().reset_index(name='NUMWAIT')

# Sort results
final_grouped_sorted = final_grouped.sort_values(by=['NUMWAIT', 'S_NAME'], ascending=[False, True])

# Write to CSV
final_grouped_sorted.to_csv('query_output.csv', index=False)
