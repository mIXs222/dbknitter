import pymongo
from pymongo import MongoClient
import direct_redis
import pandas as pd

# Connect to mongodb
client = MongoClient('mongodb://mongodb:27017/')
mongo_db = client['tpch']
supplier_col = mongo_db['supplier']
lineitem_col = mongo_db['lineitem']

# Connect to redis
r = direct_redis.DirectRedis(host='redis', port=6379, db=0)
nation_df = r.get('nation')
orders_df = r.get('orders')

# Convert MongoDB collections to DataFrames
supplier_df = pd.DataFrame(list(supplier_col.find()))
lineitem_df = pd.DataFrame(list(lineitem_col.find()))

# Filter nation_df for 'SAUDI ARABIA'
nation_df = nation_df[nation_df['N_NAME'] == 'SAUDI ARABIA']

# Filter lineitem_df with status 'F'
lineitem_df = lineitem_df[lineitem_df['L_LINESTATUS'] == 'F']

# Identify orders with multiple line items from different suppliers
order_supplier_df = lineitem_df.groupby('L_ORDERKEY')['L_SUPPKEY'].nunique()
multi_supplier_orders = order_supplier_df[order_supplier_df > 1].index

# Filter orders with multiple suppliers
multi_supplier_lineitems_df = lineitem_df[lineitem_df['L_ORDERKEY'].isin(multi_supplier_orders)]

# Find suppliers whose product was part of a multi-supplier order who failed to meet the delivery date
# and exclude orders with any other suppliers meeting the delivery date
failed_lineitems_df = multi_supplier_lineitems_df[multi_supplier_lineitems_df['L_COMMITDATE'] < multi_supplier_lineitems_df['L_RECEIPTDATE']]
final_lineitems_df = failed_lineitems_df.groupby('L_SUPPKEY').filter(lambda x: (x['L_RECEIPTDATE'] > x['L_COMMITDATE']).all())

# Count number of await lineitems (NUMWAIT) and join with supplier name
result_df = final_lineitems_df.groupby('L_SUPPKEY').size().reset_index(name='NUMWAIT').merge(supplier_df, left_on='L_SUPPKEY', right_on='S_SUPPKEY')

# Select necessary columns and sort as instructed
result_df = result_df[['NUMWAIT', 'S_NAME']].sort_values(by=['NUMWAIT', 'S_NAME'], ascending=[False, True])

# Write to CSV
result_df.to_csv('query_output.csv', index=False)
