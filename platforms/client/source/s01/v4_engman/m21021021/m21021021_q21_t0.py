# suppliers_who_kept_orders_waiting.py
from pymongo import MongoClient
import pandas as pd
from direct_redis import DirectRedis

# Connect to MongoDB
mongo_client = MongoClient('mongodb', 27017)
mongo_db = mongo_client['tpch']
lineitem_df = pd.DataFrame(list(mongo_db.lineitem.find()))

# Connect to Redis
redis_client = DirectRedis(host='redis', port=6379, db=0)
nation_df = pd.DataFrame(redis_client.get('nation'))
supplier_df = pd.DataFrame(redis_client.get('supplier'))
orders_df = pd.DataFrame(redis_client.get('orders'))

# Filter nation for SAUDI ARABIA and join with supplier
nation_filtered = nation_df[nation_df['N_NAME'] == 'SAUDI ARABIA']
supplier_nation = pd.merge(supplier_df, nation_filtered, left_on='S_NATIONKEY', right_on='N_NATIONKEY')

# Join orders with lineitem on order key and filter by order status 'F'
orders_f = orders_df[orders_df['O_ORDERSTATUS'] == 'F']
lineitem_orders = pd.merge(lineitem_df, orders_f, left_on='L_ORDERKEY', right_on='O_ORDERKEY')

# Find multi-supplier orders
multi_supplier_orders = lineitem_orders.groupby('L_ORDERKEY').filter(lambda x: x['L_SUPPKEY'].nunique() > 1)

# Find suppliers who were the only ones to not meet the commit date
def single_failing(l):
    return (l['L_COMMITDATE'] < l['L_RECEIPTDATE']).sum() == l.shape[0]

failed_suppliers = multi_supplier_orders.groupby(['L_ORDERKEY', 'L_SUPPKEY']).filter(single_failing)

# Count the number of times each supplier kept an order waiting
numwait = failed_suppliers.groupby('L_SUPPKEY').size().reset_index(name='NUMWAIT')

# Merge with supplier_nation to get supplier names and sort
result = pd.merge(numwait, supplier_nation, left_on='L_SUPPKEY', right_on='S_SUPPKEY')
result_sorted = result[['NUMWAIT', 'S_NAME']].sort_values(by=['NUMWAIT', 'S_NAME'], ascending=[False, True])

# Write output to CSV file
result_sorted.to_csv('query_output.csv', index=False)
